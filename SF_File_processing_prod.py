# Databricks notebook source
#External lib installation
databricks_artifacts_token = dbutils.secrets.get("key-vault-akv", key='databricks-artifacts-token')
%pip install --index-url https://{databricks_artifacts_token}@pkgs.dev.azure.com/travelleadersgroup/MDM/_packaging/MDM_Feed/pypi/simple/ phonenumbers

# COMMAND ----------

# MAGIC %run ./Utilities

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import phonenumbers
import re

# COMMAND ----------

USER = spark.sql("SELECT current_user() as user").collect()[0]["user"]
TS = current_timestamp()
BRONZE_HOTEL_INC_TABLE = "bronze_incremental.b_sf_hotel_incremental"
BRONZE_BRAND_INC_TABLE = "bronze_incremental.b_sf_brand_incremental"
SILVER_INC_TABLE = "silver_incremental.s_sf_hotel_incremental"
HOTEL_MASTER_GOLD = "gold.g_hotel_master"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze processing

# COMMAND ----------

# DBTITLE 1,Read raw file
path_hotel = "/mnt/raw/Salesforce/Input_Files/Hotel/"
path_brand = "/mnt/raw/Salesforce/Input_Files/Brand/"

df_hotel = spark.read.format("parquet").load(path_hotel).withColumn("sourcefile", regexp_extract(input_file_name(), r"([^/]+$)", 1))
df_brand = spark.read.format("parquet").load(path_brand).withColumn("sourcefile", regexp_extract(input_file_name(), r"([^/]+$)", 1))

# COMMAND ----------

# DBTITLE 1,Get Latest Files & Set timestamp
# Get the file with the latest timestamp 
latest_src_file_hotel = df_hotel.filter(col("lastmodifieddate").isNotNull()) \
                     .orderBy(col("lastmodifieddate").desc()) \
                     .select("sourcefile") \
                     .first()

latest_src_file_brand = df_brand.filter(col("lastmodifieddate").isNotNull()) \
                     .orderBy(col("lastmodifieddate").desc()) \
                     .select("sourcefile") \
                     .first()

start_ts_utc = datetime.now().isoformat()

if latest_src_file_hotel:
    file_name_hotel = latest_src_file_hotel[0]
    print(f"Latest file: {file_name_hotel}")
else:
    print("No valid files found")

if latest_src_file_brand:
    file_name_brand = latest_src_file_brand[0]
    print(f"Latest file: {file_name_brand}")
else:
    print("No valid files found")

print(start_ts_utc)

# COMMAND ----------

# DBTITLE 1,Load Latest File Data
df_hotel = df_hotel.filter(col("sourcefile") == file_name_hotel)
df_brand = df_brand.filter(col("sourcefile") == file_name_brand)

# COMMAND ----------

# DBTITLE 1,Sanitize
bronze_hotel_df = sanitize_column_names(df_hotel)
bronze_brand_df = sanitize_column_names(df_brand)

# COMMAND ----------

# DBTITLE 1,Add Audit fields
bronze_hotel_df = add_audit_columns_bronze(bronze_hotel_df, USER, TS, file_name_hotel)
bronze_brand_df = add_audit_columns_bronze(bronze_brand_df, USER, TS, file_name_brand)

# COMMAND ----------

# DBTITLE 1,Load Bronze
print("Writing to Bronze layer...")
bronze_hotel_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_HOTEL_INC_TABLE)
bronze_brand_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_BRAND_INC_TABLE)
print("Loaded to Bronze layer...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer processing

# COMMAND ----------

print("#"*50)
print("Loading from Bronze for transformation...")
bronze_hotel_df = spark.table(BRONZE_HOTEL_INC_TABLE)
bronze_brand_df = spark.table(BRONZE_BRAND_INC_TABLE)
df_hotel = bronze_hotel_df.filter((to_date(col('lastupdatedtimestamp')) == current_date()) & (col('filename') == file_name_hotel))
print("Loading new data from Bronze for transformation...Done")

# COMMAND ----------

# DBTITLE 1,Integrate SF source brand dataframe
df = (
    df_hotel.join(
        bronze_brand_df.select(col("id"), col("name").alias("brandname")).dropDuplicates(),
        df_hotel.hotelbrandc == bronze_brand_df.id,
        how="left"
    )
    .drop(bronze_brand_df.id)  # remove id from brand dataframe
)

# COMMAND ----------

# Create code field based on id
df = standardize_code_field(df, "id", "SF")

# COMMAND ----------

# DBTITLE 1,Enrichment with reference data
# Data Enrichment
df = enrich_with_brand_country_reference_data(df, spark, 'countryc', 'brandname',  'iso3char', 'brbrandname')

# COMMAND ----------

# DBTITLE 1,Standardize reference columns
# Create code field based on id
df = standardize_code_field(df, "id", "SF")

# Standardize referance data
df = standardize_reference_column(df, column="isocountryname", fallback_col="countryc")
df = standardize_reference_column(df, column="brbrandname", fallback_col="brandname")
df = standardize_reference_column(df, column="brparentchainname", fallback_col="chaincodemasterchainc")

# COMMAND ----------

# DBTITLE 1,Standardise Phone Number
# Apply the pandas UDF for each column
df = df.withColumn("phone", concat_ws("",df.phonecountrycodec.cast(IntegerType()),df.phoneareacodec.cast(IntegerType()),df.phonenumberc.cast(IntegerType())))

df = (
    df.withColumn("phone", remove_special_characters_udf(col("phone")))
      .withColumn(
          "formattedphone",
          format_phone_number_pandas_udf(col("phone"), col("iso2char"))
      )
      .withColumn(
          "generalmanagerphonec",
          remove_special_characters_udf(col("generalmanagerphonec"))
      )
      .withColumn(
          "formattedphonegeneralmanagerphonec",
          format_phone_number_pandas_udf(col("generalmanagerphonec"), col("iso2char"))
      )
      .withColumn(
          "frontdeskphonec",
          remove_special_characters_udf(col("frontdeskphonec"))
      )
      .withColumn(
          "formattedphonefrontdeskphonec",
          format_phone_number_pandas_udf(col("frontdeskphonec"), col("iso2char"))
      )
      .withColumn(
          "roomreservationsphonec",
          remove_special_characters_udf(col("roomreservationsphonec"))
      )
      .withColumn(
          "formattedroomreservationsphonec",
          format_phone_number_pandas_udf(col("roomreservationsphonec"), col("iso2char"))
      ).drop(
        "phone",
        "generalmanagerphonec", 
        "frontdeskphonec", 
        "roomreservationsphonec"
    )
)

# Get Six digit phone number
df = six_digit_phonenumber(df, "formattedphone")

# COMMAND ----------

# DBTITLE 1,Build Full Address
#Building full address...
print("Building full address...")

# Pass address details to the function
address_fields = ("address1c", "address2c", "cityc", "stateprovincecodec", "iso2char", "countryc_standardized", "postalcodec")

df = build_full_address_column(df, fields=address_fields)

# COMMAND ----------

# DBTITLE 1,Standardize GDS Code
# Map each property-code column to the brand column it needs (city not used here)
column_configs = {
    "amadeuspropertycodec": {"brand_col": "amadeuschaincodec", "city_col": None},
    "sabrepropertycodec": {"brand_col": None, "city_col": None},
    "apollogalileopropertycodec": {"brand_col": None, "city_col": None},
    "worldspanpropertycodec": {"brand_col": "worldspanchaincodec", "city_col": None},
}

# Run the standardization (creates *_standardized columns, preserves originals)
df = standardize_gds_columns(
    df,
    column_configs=column_configs,
    overwrite=False,              # keep originals
    suffix="_standardized"        # write to new columns
)

# COMMAND ----------

# DBTITLE 1,Additional transformations
df = (
    df
    .withColumn(
        "formattedCheckInTimec",
        concat_ws("", lpad(hour(col("checkintimec")), 2, '0'), lpad(minute(col("checkintimec")), 2, '0'))
    )
    .withColumn(
        "formattedCheckOutTimec",
        concat_ws("", lpad(hour(col("checkouttimec")), 2, '0'), lpad(minute(col("checkouttimec")), 2, '0'))
    )
    .withColumn(
        "SELECTHotelFlag",
        expr("CASE WHEN substring(upper(trim(coalesce(programaffiliationc,''))), 1, 1) = 'S' THEN 'Y' ELSE '' END")
    )
    .withColumn(
        "CuratedHotelFlag",
        expr("CASE WHEN substring(upper(trim(coalesce(programaffiliationc,''))), 1, 1) = 'C' THEN 'Y' ELSE '' END")
    )
)

# COMMAND ----------

df = df.na.fill('')

# COMMAND ----------

df.createOrReplaceTempView("vw_latest_salesforce")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_INC_TABLE} AS
SELECT
  Code,
  name,
  name as HotelName,
  brandname,
  brbrandcode,
  address1c,
  address2c,
  cityc,
  stateprovincecodec,
  isocountryname,
  iso2char,
  countryc,
  postalcodec,
  StreetAddressFullAddressLine,
  formattedphone,
  hotelwebsitec,
  hotelmarkettierc,
  amadeuschaincodec,
  apollogalileochaincodec,
  sabrechaincodec,
  worldspanchaincodec,
  latitudec,
  longitudec,
  selectrfpratingnumberc,
  chaincodemasterchainc,
  formattedphonefrontdeskphonec,
  formattedphonegeneralmanagerphonec,
  arrivalsdeparturesprotocolsc,
  billingaddressc,
  billingcityc,
  billingcountryc,
  billingpostalcodec,
  billingstateprovincec,
  businesscenterc,
  formattedcheckintimec,
  formattedcheckouttimec,
  clientspecificpropertycontactc,
  clientspecificpropertytitlec,
  conciergetourdeskc,
  extendedstayresidentialapartmentc,
  hotelservicec,
  id,
  imfcurrencycodec,
  mailingaddress1c,
  mailingaddress2c,
  mailingcityc,
  mailingcountryc,
  mailingpostalcodec,
  mailingstateprovincecodec,
  selectrfpminimumnightstayc,
  nearestairportcodec,
  occupancytaxc,
  selectrfpcrownratingc,
  pleasespecifiyc,
  petfriendlyregulationsc,
  selectrfppricerangec,
  propertycodec,
  propertydescriptionc,
  selectrfprepresentationcompanyc,
  resortfeeincludesc,
  roomreservationsemailc,
  formattedroomreservationsphonec,
  taxesc,
  totalnumberfloorsc,
  totalnumberroomssuitesc,
  yearoflastguestroomrenovac,
  yearpropertybuiltc,
  programaffiliationc,
  SELECTHotelFlag,
  CuratedHotelFlag,
  citytaxc,
  citytaxpercentorfixedc,
  occupancytaxinnegotiatedratec,
  occupancytaxpercentorfixedc,
  resortfeec,
  countryc_standardized,
  brandname_standardized,
  brparentchaincode,
  chaincodemasterchainc_standardized,
  sixdigitphonenumber,
  amadeuspropertycodec_standardized,
  sabrepropertycodec_standardized,
  apollogalileopropertycodec_standardized,
  worldspanpropertycodec_standardized,
  lastmodifieddate,
  current_user() AS createdby,
  current_user() AS lastupdatedby,
  current_timestamp() AS createdtimestamp,
  current_timestamp() AS lastupdatedtimestamp
FROM vw_latest_salesforce
WHERE 1=0
""")

# COMMAND ----------

# DBTITLE 1,Load Silver
spark.sql(f"""
MERGE INTO {SILVER_INC_TABLE} AS target
USING vw_latest_salesforce AS source
ON target.Code = source.Code

WHEN MATCHED THEN UPDATE SET
  target.name                                   = source.name,
  target.HotelName                              = source.name,
  target.brandname                              = source.brandname,
  target.brbrandcode                            = source.brbrandcode,
  target.address1c                              = source.address1c,
  target.address2c                              = source.address2c,
  target.cityc                                  = source.cityc,
  target.stateprovincecodec                     = source.stateprovincecodec,
  target.isocountryname                         = source.isocountryname,
  target.iso2char                               = source.iso2char,
  target.countryc                               = source.countryc,
  target.postalcodec                            = source.postalcodec,
  target.StreetAddressFullAddressLine           = source.StreetAddressFullAddressLine,
  target.formattedphone                         = source.formattedphone,
  target.hotelwebsitec                          = source.hotelwebsitec,
  target.hotelmarkettierc                       = source.hotelmarkettierc,
  target.amadeuschaincodec                      = source.amadeuschaincodec,
  target.apollogalileochaincodec                = source.apollogalileochaincodec,
  target.sabrechaincodec                        = source.sabrechaincodec,
  target.worldspanchaincodec                    = source.worldspanchaincodec,
  target.latitudec                              = source.latitudec,
  target.longitudec                             = source.longitudec,
  target.selectrfpratingnumberc                 = source.selectrfpratingnumberc,
  target.chaincodemasterchainc                  = source.chaincodemasterchainc,
  target.formattedphonefrontdeskphonec          = source.formattedphonefrontdeskphonec,
  target.formattedphonegeneralmanagerphonec     = source.formattedphonegeneralmanagerphonec,
  target.arrivalsdeparturesprotocolsc           = source.arrivalsdeparturesprotocolsc,
  target.billingaddressc                        = source.billingaddressc,
  target.billingcityc                           = source.billingcityc,
  target.billingcountryc                        = source.billingcountryc,
  target.billingpostalcodec                     = source.billingpostalcodec,
  target.billingstateprovincec                  = source.billingstateprovincec,
  target.businesscenterc                        = source.businesscenterc,
  target.formattedcheckintimec                  = source.formattedcheckintimec,
  target.formattedcheckouttimec                 = source.formattedcheckouttimec,
  target.clientspecificpropertycontactc         = source.clientspecificpropertycontactc,
  target.clientspecificpropertytitlec           = source.clientspecificpropertytitlec,
  target.conciergetourdeskc                     = source.conciergetourdeskc,
  target.extendedstayresidentialapartmentc      = source.extendedstayresidentialapartmentc,
  target.hotelservicec                          = source.hotelservicec,
  target.id                                     = source.id,
  target.imfcurrencycodec                       = source.imfcurrencycodec,
  target.mailingaddress1c                       = source.mailingaddress1c,
  target.mailingaddress2c                       = source.mailingaddress2c,
  target.mailingcityc                           = source.mailingcityc,
  target.mailingcountryc                        = source.mailingcountryc,
  target.mailingpostalcodec                     = source.mailingpostalcodec,
  target.mailingstateprovincecodec              = source.mailingstateprovincecodec,
  target.selectrfpminimumnightstayc             = source.selectrfpminimumnightstayc,
  target.nearestairportcodec                    = source.nearestairportcodec,
  target.occupancytaxc                          = source.occupancytaxc,
  target.selectrfpcrownratingc                  = source.selectrfpcrownratingc,
  target.pleasespecifiyc                        = source.pleasespecifiyc,
  target.petfriendlyregulationsc                = source.petfriendlyregulationsc,
  target.selectrfppricerangec                   = source.selectrfppricerangec,
  target.propertycodec                          = source.propertycodec,
  target.propertydescriptionc                   = source.propertydescriptionc,
  target.selectrfprepresentationcompanyc        = source.selectrfprepresentationcompanyc,
  target.resortfeeincludesc                     = source.resortfeeincludesc,
  target.roomreservationsemailc                 = source.roomreservationsemailc,
  target.formattedroomreservationsphonec        = source.formattedroomreservationsphonec,
  target.taxesc                                 = source.taxesc,
  target.totalnumberfloorsc                     = source.totalnumberfloorsc,
  target.totalnumberroomssuitesc                = source.totalnumberroomssuitesc,
  target.yearoflastguestroomrenovac             = source.yearoflastguestroomrenovac,
  target.yearpropertybuiltc                     = source.yearpropertybuiltc,
  target.programaffiliationc                    = source.programaffiliationc,
  target.SELECTHotelFlag                        = source.SELECTHotelFlag,    
  target.CuratedHotelFlag                       = source.CuratedHotelFlag,
  target.citytaxc                               = source.citytaxc,
  target.citytaxpercentorfixedc                 = source.citytaxpercentorfixedc,
  target.occupancytaxinnegotiatedratec          = source.occupancytaxinnegotiatedratec,
  target.occupancytaxpercentorfixedc            = source.occupancytaxpercentorfixedc,
  target.resortfeec                             = source.resortfeec,
  target.countryc_standardized                  = source.countryc_standardized,
  target.brandname_standardized                 = source.brandname_standardized,
  target.brparentchaincode                      = source.brparentchaincode,
  target.chaincodemasterchainc_standardized     = source.chaincodemasterchainc_standardized,
  target.sixdigitphonenumber                    = source.sixdigitphonenumber,
  target.amadeuspropertycodec_standardized      = source.amadeuspropertycodec_standardized,
  target.sabrepropertycodec_standardized        = source.sabrepropertycodec_standardized,
  target.apollogalileopropertycodec_standardized= source.apollogalileopropertycodec_standardized,
  target.worldspanpropertycodec_standardized    = source.worldspanpropertycodec_standardized,
  target.lastmodifieddate                       = source.lastmodifieddate,
  target.lastupdatedby                          = current_user(),
  target.lastupdatedtimestamp                   = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
  Code,
  name,
  HotelName,
  brandname,
  brbrandcode,
  address1c,
  address2c,
  cityc,
  stateprovincecodec,
  isocountryname,
  iso2char,
  countryc,
  postalcodec,
  StreetAddressFullAddressLine,
  formattedphone,
  hotelwebsitec,
  hotelmarkettierc,
  amadeuschaincodec,
  apollogalileochaincodec,
  sabrechaincodec,
  worldspanchaincodec,
  latitudec,
  longitudec,
  selectrfpratingnumberc,
  chaincodemasterchainc,
  formattedphonefrontdeskphonec,
  formattedphonegeneralmanagerphonec,
  arrivalsdeparturesprotocolsc,
  billingaddressc,
  billingcityc,
  billingcountryc,
  billingpostalcodec,
  billingstateprovincec,
  businesscenterc,
  formattedcheckintimec,
  formattedcheckouttimec,
  clientspecificpropertycontactc,
  clientspecificpropertytitlec,
  conciergetourdeskc,
  extendedstayresidentialapartmentc,
  hotelservicec,
  id,
  imfcurrencycodec,
  mailingaddress1c,
  mailingaddress2c,
  mailingcityc,
  mailingcountryc,
  mailingpostalcodec,
  mailingstateprovincecodec,
  selectrfpminimumnightstayc,
  nearestairportcodec,
  occupancytaxc,
  selectrfpcrownratingc,
  pleasespecifiyc,
  petfriendlyregulationsc,
  selectrfppricerangec,
  propertycodec,
  propertydescriptionc,
  selectrfprepresentationcompanyc,
  resortfeeincludesc,
  roomreservationsemailc,
  formattedroomreservationsphonec,
  taxesc,
  totalnumberfloorsc,
  totalnumberroomssuitesc,
  yearoflastguestroomrenovac,
  yearpropertybuiltc,
  programaffiliationc,
  SELECTHotelFlag,
  CuratedHotelFlag,
  citytaxc,
  citytaxpercentorfixedc,
  occupancytaxinnegotiatedratec,
  occupancytaxpercentorfixedc,
  resortfeec,
  countryc_standardized,
  brandname_standardized,
  brparentchaincode,
  chaincodemasterchainc_standardized,
  sixdigitphonenumber,
  amadeuspropertycodec_standardized,
  sabrepropertycodec_standardized,
  apollogalileopropertycodec_standardized,
  worldspanpropertycodec_standardized,
  lastmodifieddate,
  createdby,
  lastupdatedby,
  createdtimestamp,
  lastupdatedtimestamp
) VALUES (
  source.Code,
  source.name,
  source.name,  -- HotelName
  source.brandname,
  source.brbrandcode,
  source.address1c,
  source.address2c,
  source.cityc,
  source.stateprovincecodec,
  source.isocountryname,
  source.iso2char,
  source.countryc,
  source.postalcodec,
  source.StreetAddressFullAddressLine,
  source.formattedphone,
  source.hotelwebsitec,
  source.hotelmarkettierc,
  source.amadeuschaincodec,
  source.apollogalileochaincodec,
  source.sabrechaincodec,
  source.worldspanchaincodec,
  source.latitudec,
  source.longitudec,
  source.selectrfpratingnumberc,
  source.chaincodemasterchainc,
  source.formattedphonefrontdeskphonec,
  source.formattedphonegeneralmanagerphonec,
  source.arrivalsdeparturesprotocolsc,
  source.billingaddressc,
  source.billingcityc,
  source.billingcountryc,
  source.billingpostalcodec,
  source.billingstateprovincec,
  source.businesscenterc,
  source.formattedcheckintimec,
  source.formattedcheckouttimec,
  source.clientspecificpropertycontactc,
  source.clientspecificpropertytitlec,
  source.conciergetourdeskc,
  source.extendedstayresidentialapartmentc,
  source.hotelservicec,
  source.id,
  source.imfcurrencycodec,
  source.mailingaddress1c,
  source.mailingaddress2c,
  source.mailingcityc,
  source.mailingcountryc,
  source.mailingpostalcodec,
  source.mailingstateprovincecodec,
  source.selectrfpminimumnightstayc,
  source.nearestairportcodec,
  source.occupancytaxc,
  source.selectrfpcrownratingc,
  source.pleasespecifiyc,
  source.petfriendlyregulationsc,
  source.selectrfppricerangec,
  source.propertycodec,
  source.propertydescriptionc,
  source.selectrfprepresentationcompanyc,
  source.resortfeeincludesc,
  source.roomreservationsemailc,
  source.formattedroomreservationsphonec,
  source.taxesc,
  source.totalnumberfloorsc,
  source.totalnumberroomssuitesc,
  source.yearoflastguestroomrenovac,
  source.yearpropertybuiltc,
  source.programaffiliationc,
  source.SELECTHotelFlag,
  source.CuratedHotelFlag,
  source.citytaxc,
  source.citytaxpercentorfixedc,
  source.occupancytaxinnegotiatedratec,
  source.occupancytaxpercentorfixedc,
  source.resortfeec,
  source.countryc_standardized,
  source.brandname_standardized,
  source.brparentchaincode,
  source.chaincodemasterchainc_standardized,
  source.sixdigitphonenumber,
  source.amadeuspropertycodec_standardized,
  source.sabrepropertycodec_standardized,
  source.apollogalileopropertycodec_standardized,
  source.worldspanpropertycodec_standardized,
  source.lastmodifieddate,
  current_user(),
  current_user(),
  current_timestamp(),
  current_timestamp()
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer (HotelMaster)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HOTEL_MASTER_GOLD} (
    AccountingManagerName STRING,
    AccountingManagerEmail STRING,
    AccountsEmail STRING,
    AccountsFaxNumber STRING,
    AccountsPhoneNumber STRING,
    Address1 STRING,
    Address2 STRING,
    Address3 STRING,
    Address4 STRING,
    AirportCityCode STRING,
    AmadeusBrandCode STRING,
    AmadeusPropertyCode STRING,
    ApolloOrGalileoBrandCode STRING,
    ApolloOrGalileoPropertyCode STRING,
    ArrivalDepartureProtocols STRING,
    BillingAddress1 STRING,
    BillingAddress2 STRING,
    BillingAddress3 STRING,
    BillingAddress4 STRING,
    BillingCity STRING,
    BillingContactEmail STRING,
    BillingContactName STRING,
    BillingContactPhoneNumber STRING,
    BillingCountryName STRING,
    BillingCounty STRING,
    BillingPostalCode STRING,
    BillingRegion STRING,
    BillingStateOrProvince STRING,
    BillingSupplier STRING,
    BlacklistedProperty STRING,
    BrandCode STRING,
    BrandName STRING,
    BrandNameAlternate STRING,
    BusinessCenter STRING,
    BusinessType STRING,
    CancellationPolicy STRING,
    CheckInTime STRING,
    CheckOutTime STRING,
    City STRING,
    CityCode STRING,
    CityTax STRING,
    CityTaxPercentFixed STRING,
    ClearMatching STRING,
    ClientSpecificPropertyContact STRING,
    ClientSpecificPropertyTitle STRING,
    ClientSpecificSalesEmail STRING,
    ClientSpecificSalesFaxNumber STRING,
    ClientSpecificSalesPhoneNumber STRING,
    Code STRING,
    CollectionRestrictionTerms STRING,
    CommissionableNegotiatedRate STRING,
    CommissionableNegotiatedRateInGDS STRING,
    CommissionableNegotiatedRatePercent STRING,
    CommissionEmail STRING,
    CommissionFaxNumber STRING,
    CommissionPhoneNumber STRING,
    CommissionsOrAccountsPayableEmail1 STRING,
    CommissionsOrAccountsPayableEmail2 STRING,
    CommissionsOrAccountsPayableEmail3 STRING,
    ConciergeOrTourDesk STRING,
    CountryCode STRING,
    CountryName STRING,
    County STRING,
    CuratedHotelFlag STRING,
    CuratedProgramBillingContactEmail STRING,
    CuratedProgramBillingContactName STRING,
    CuratedProgramCommissionContactEmail STRING,
    CuratedProgramCommissionContactName STRING,
    CuratedProgramPrimaryContactEmail STRING,
    CuratedProgramPrimaryContactName STRING,
    CuratedProgramSecondaryContactEmail STRING,
    CuratedProgramSecondaryContactName STRING,
    CurateHotelLogo STRING,
    EarlyCheckOutFee STRING,
    EarlyCheckOutFeeinNegotiatedRate STRING,
    EarlyCheckOutFeePercentOrFixed STRING,
    EfileIndicator STRING,
    EnterDTM STRING,
    EntertainmentSalesContactEmail STRING,
    EntertainmentSalesContactName STRING,
    EnterUserID STRING,
    EnterUserName STRING,
    ExcludefromtheHotelDirectory STRING,
    ExtendedStayOrResidentialApartment STRING,
    FormattedAddress STRING,
    FrontDeskHours STRING,
    FrontDeskPhoneNumber STRING,
    FullOrLimitedService STRING,
    GDSChain STRING,
    GDSSource STRING,
    GeneralEmail STRING,
    GeneralManagerEmail STRING,
    GeneralManagerName STRING,
    GeneralManagerPhoneNumber STRING,
    GeneralManagerTitle STRING,
    GlobalAccountManagerEmail STRING,
    GlobalAccountManagerName STRING,
    GroupCommissionContactRest STRING,
    GroupCommissionContactUS STRING,
    GroupSalesContactEmail STRING,
    GroupSalesContactName STRING,
    GroupsEmail STRING,
    GroupsFaxNumber STRING,
    GroupsPhoneNumber STRING,
    HotelChain STRING,
    HotelManagementCompany STRING,
    HotelName STRING,
    HotelOwnershipCompany STRING,
    HotelSelectRFPBillingAddressLine1 STRING,
    HotelSelectRFPBillingCity STRING,
    HotelSelectRFPBillingCompany STRING,
    HotelSelectRFPBillingContactEmail STRING,
    HotelSelectRFPBillingContactName STRING,
    HotelSelectRFPBillingContactTitle STRING,
    HotelSelectRFPBillingCountry STRING,
    HotelSelectRFPBillingName STRING,
    HotelSelectRFPBillingPurchaseOrderNumber STRING,
    HotelSelectRFPBillingStateOrProvince STRING,
    HotelSelectRFPBillingVATNumber STRING,
    HotelSelectRFPBillingZipOrPostalCode STRING,
    HotelStatus STRING,
    ID STRING,
    IDAndrewHarper STRING,
    IDBW STRING,
    IDCurated STRING,
    IDHotelMDMRef STRING,
    IDHPI STRING,
    IDHyattHotel STRING,
    IDIHG STRING,
    IDLanyon STRING,
    IDNetsuite STRING,
    IDNetsuiteDev STRING,
    IDNetsuiteQA1 STRING,
    IDNetsuiteQA2 STRING,
    IDNetsuiteQA3 STRING,
    IDOnyx STRING,
    IDSalesforce STRING,
    IDSELECT STRING,
    IDTACS STRING,
    IDWorldWide STRING,
    IDWPS STRING,
    IMFCurrencyCode STRING,
    InternovaHotelProgram STRING,
    IsDeleted STRING,
    ITGAccountManagerEmail STRING,
    ITGAccountManagerName STRING,
    LastChgDTM STRING,
    LastChgUserID STRING,
    LastChgUserName STRING,
    Latitude STRING,
    LeadCommissionDepartmentContactCentral STRING,
    Locked STRING,
    Longitude STRING,
    MailingAddress1 STRING,
    MailingAddress2 STRING,
    MailingAddress3 STRING,
    MailingAddress4 STRING,
    MailingCity STRING,
    MailingCountryName STRING,
    MailingPostalCode STRING,
    MailingStateOrProvinceCode STRING,
    MainFaxNumber STRING,
    MainPhoneNumber STRING,
    ManagementRepresentativeEmail STRING,
    ManagementRepresentativeName STRING,
    ManagementRepresentativePhoneNumber STRING,
    ManagementRepresentativeTitle STRING,
    MapURL STRING,
    MarketingEmail STRING,
    MarketingName STRING,
    MarketingPhoneNumber STRING,
    MarketingTitle STRING,
    MarketTier STRING,
    MergeSequenceNumber STRING,
    Minimumnightstay STRING,
    Name STRING,
    NearestAirportCode STRING,
    OccupancyTax STRING,
    OccupancyTaxinNegotiatedRate STRING,
    OccupancyTaxPercentOrFixed STRING,
    OfficalRatingAgency STRING,
    OfficalRatingAgencyOther STRING,
    OnyxFlag STRING,
    OnyxInactive STRING,
    OtherTax STRING,
    OtherTaxDescription STRING,
    OtherTaxPercentOrFixed STRING,
    ParentChainCode STRING,
    ParentChainName STRING,
    ParkingCost STRING,
    ParkingDescription STRING,
    ParkingOrValetProtocols STRING,
    PartnershipContactInternationalEmail STRING,
    PartnershipContactInternationalName STRING,
    PartnershipContactUKEmail STRING,
    PartnershipContactUKName STRING,
    PartnershipContactUSEmail STRING,
    PartnershipContactUSname STRING,
    PaymentComingFrom STRING,
    PaymentProviderUpload STRING,
    PaymentVendor STRING,
    PetFriendly STRING,
    PostalCode STRING,
    PostStayCommissionEmail STRING,
    PriceRange STRING,
    Priority STRING,
    PropertyCode STRING,
    PropertyDescription STRING,
    PropertyLocationCode STRING,
    PropertySalesOrGeneralEmail STRING,
    RatingProgram STRING,
    Region STRING,
    Representation STRING,
    ReservationEmail1 STRING,
    ReservationEmail2 STRING,
    ReservationEmail3 STRING,
    ReservationsPhoneNumber STRING,
    ResortFee STRING,
    ResortFeeIncludes STRING,
    RFPRepresentationCompany STRING,
    SabreBrandCode STRING,
    SabrePropertyCode STRING,
    SalesEmail STRING,
    SalesName STRING,
    SalesPhoneNumber STRING,
    SalesTitle STRING,
    SecondaryChain STRING,
    SelectBillingContactEmail STRING,
    SelectBillingContactName STRING,
    SelectCommissionContactEmail STRING,
    SelectCommissionContactName STRING,
    SELECTHotelFlag STRING,
    SelectHotelLogo STRING,
    SelectPrimaryContactEmail STRING,
    SelectPrimaryContactName STRING,
    SelectSecondaryContactEmail STRING,
    SelectSecondaryContactName STRING,
    ServiceFee STRING,
    ServiceFeePercentOrFixed STRING,
    SixDigitPhoneNumber STRING,
    SourceSystem STRING,
    SplitSequenceNumber STRING,
    StarRating STRING,
    StateFullName STRING,
    StateOrProvince STRING,
    StateTax STRING,
    StateTaxPercentOrFixed STRING,
    StreetAddressFullAddressLine STRING,
    Taxes STRING,
    TotalNumberAccessibleRoomsOrSuites STRING,
    TotalNumberFloors STRING,
    TotalNumberNonSmokingRoomsOrSuites STRING,
    TotalNumberRoomsOrSuites STRING,
    UBRHash STRING,
    UBRID STRING,
    UpdatedEmail STRING,
    UTCOffsetinMinutes STRING,
    ValidationStatusID STRING,
    VATOrGST STRING,
    VATOrGSTPercentOrFixed STRING,
    Website STRING,
    WorldspanBrandCode STRING,
    WorldspanPropertyCode STRING,
    WorldWideHotelFlag STRING,
    WorldWideHotelLogo STRING,
    WWHPBillingContactEmail STRING,
    WWHPBillingContactName STRING,
    WWHPCommissionContactEmail STRING,
    WWHPCommissionContactName STRING,
    WWHPProgramContactEmail STRING,
    WWHPProgramContactName STRING,
    YearOfLastRenovation STRING,
    YearPropertyBuilt STRING
)
""")

# COMMAND ----------

# DBTITLE 1,Load Gold
spark.sql(f"""
MERGE INTO {HOTEL_MASTER_GOLD} AS target
USING (
  SELECT
    Code                                        AS Code,
    name                                        AS Name,
    name                                        AS HotelName,
    brandname                                   AS BrandName,
    brbrandcode                                 AS BrandCode,
    address1c                                   AS Address1,
    address2c                                   AS Address2,
    cityc                                       AS City,
    stateprovincecodec                          AS StateOrProvince,
    iso2char                                    AS CountryCode,
    countryc_standardized                       AS CountryName,
    postalcodec                                 AS PostalCode,
    StreetAddressFullAddressLine                AS StreetAddressFullAddressLine,
    formattedphone                              AS MainPhoneNumber,
    hotelwebsitec                               AS Website,
    hotelmarkettierc                            AS MarketTier,
    amadeuschaincodec                           AS AmadeusBrandCode,
    amadeuspropertycodec_standardized           AS AmadeusPropertyCode,
    apollogalileochaincodec                     AS ApolloOrGalileoBrandCode,
    apollogalileopropertycodec_standardized     AS ApolloOrGalileoPropertyCode,
    sabrechaincodec                             AS SabreBrandCode,
    sabrepropertycodec_standardized             AS SabrePropertyCode,
    worldspanchaincodec                         AS WorldspanBrandCode,
    worldspanpropertycodec_standardized         AS WorldspanPropertyCode,
    latitudec                                   AS Latitude,
    longitudec                                  AS Longitude,
    selectrfpratingnumberc                      AS StarRating,
    chaincodemasterchainc_standardized          AS ParentChainName,
    formattedphonefrontdeskphonec               AS FrontDeskPhoneNumber,
    formattedphonegeneralmanagerphonec          AS GeneralManagerPhoneNumber,
    arrivalsdeparturesprotocolsc                AS ArrivalDepartureProtocols,
    billingaddressc                             AS BillingAddress1,
    billingcityc                                AS BillingCity,
    billingcountryc                             AS BillingCountryName,
    billingpostalcodec                          AS BillingPostalCode,
    billingstateprovincec                       AS BillingStateOrProvince,
    businesscenterc                             AS BusinessCenter,
    formattedcheckintimec                       AS CheckInTime,
    formattedcheckouttimec                      AS CheckOutTime,
    clientspecificpropertycontactc              AS ClientSpecificPropertyContact,
    clientspecificpropertytitlec                AS ClientSpecificPropertyTitle,
    conciergetourdeskc                          AS ConciergeOrTourDesk,
    extendedstayresidentialapartmentc           AS ExtendedStayOrResidentialApartment,
    hotelservicec                               AS FullOrLimitedService,
    id                                          AS IDSalesforce,
    imfcurrencycodec                            AS IMFCurrencyCode,
    mailingaddress1c                            AS MailingAddress1,
    mailingaddress2c                            AS MailingAddress2,
    mailingcityc                                AS MailingCity,
    mailingcountryc                             AS MailingCountryName,
    mailingpostalcodec                          AS MailingPostalCode,
    mailingstateprovincecodec                   AS MailingStateOrProvinceCode,
    selectrfpminimumnightstayc                  AS Minimumnightstay,
    nearestairportcodec                         AS NearestAirportCode,
    occupancytaxc                               AS OccupancyTax,
    selectrfpcrownratingc                       AS OfficalRatingAgency,
    pleasespecifiyc                             AS OfficalRatingAgencyOther,
    petfriendlyregulationsc                     AS PetFriendly,
    selectrfppricerangec                        AS PriceRange,
    propertycodec                               AS PropertyCode,
    propertydescriptionc                        AS PropertyDescription,
    selectrfprepresentationcompanyc             AS RFPRepresentationCompany,
    resortfeeincludesc                          AS ResortFeeIncludes,
    roomreservationsemailc                      AS ReservationEmail1,
    formattedroomreservationsphonec             AS ReservationsPhoneNumber,
    taxesc                                      AS Taxes,
    totalnumberfloorsc                          AS TotalNumberFloors,
    totalnumberroomssuitesc                     AS TotalNumberRoomsOrSuites,
    yearoflastguestroomrenovac                  AS YearOfLastRenovation,
    yearpropertybuiltc                          AS YearPropertyBuilt,
    SELECTHotelFlag                             AS SELECTHotelFlag,
    CuratedHotelFlag                            AS CuratedHotelFlag,
    citytaxc                                    AS CityTax,
    citytaxpercentorfixedc                      AS CityTaxPercentFixed,
    occupancytaxinnegotiatedratec               AS OccupancyTaxinNegotiatedRate,
    occupancytaxpercentorfixedc                 AS OccupancyTaxPercentOrFixed,
    resortfeec                                  AS ResortFee,
    brparentchaincode                           AS ParentChainCode,
    sixdigitphonenumber                         AS SixDigitPhoneNumber,
    'Salesforce'                                AS SourceSystem
  FROM {SILVER_INC_TABLE}
  WHERE lastupdatedtimestamp >= '{start_ts_utc}'
) AS source
ON target.Code = source.Code

WHEN MATCHED THEN UPDATE SET
  target.Name                                 = source.Name,
  target.HotelName                            = source.HotelName,
  target.BrandName                            = source.BrandName,
  target.BrandCode                            = source.BrandCode,
  target.Address1                             = source.Address1,
  target.Address2                             = source.Address2,
  target.City                                 = source.City,
  target.StateOrProvince                      = source.StateOrProvince,
  target.CountryName                          = source.CountryName,
  target.CountryCode                          = source.CountryCode,
  target.PostalCode                           = source.PostalCode,
  target.StreetAddressFullAddressLine         = source.StreetAddressFullAddressLine,
  target.MainPhoneNumber                      = source.MainPhoneNumber,
  target.Website                              = source.Website,
  target.MarketTier                           = source.MarketTier,
  target.AmadeusBrandCode                     = source.AmadeusBrandCode,
  target.AmadeusPropertyCode                  = source.AmadeusPropertyCode,
  target.ApolloOrGalileoBrandCode             = source.ApolloOrGalileoBrandCode,
  target.ApolloOrGalileoPropertyCode          = source.ApolloOrGalileoPropertyCode,
  target.SabreBrandCode                       = source.SabreBrandCode,
  target.SabrePropertyCode                    = source.SabrePropertyCode,
  target.WorldspanBrandCode                   = source.WorldspanBrandCode,
  target.WorldspanPropertyCode                = source.WorldspanPropertyCode,
  target.Latitude                             = source.Latitude,
  target.Longitude                            = source.Longitude,
  target.StarRating                           = source.StarRating,
  target.ParentChainName                      = source.ParentChainName,
  target.FrontDeskPhoneNumber                 = source.FrontDeskPhoneNumber,
  target.GeneralManagerPhoneNumber            = source.GeneralManagerPhoneNumber,
  target.ArrivalDepartureProtocols            = source.ArrivalDepartureProtocols,
  target.BillingAddress1                      = source.BillingAddress1,
  target.BillingCity                          = source.BillingCity,
  target.BillingCountryName                   = source.BillingCountryName,
  target.BillingPostalCode                    = source.BillingPostalCode,
  target.BillingStateOrProvince               = source.BillingStateOrProvince,
  target.BusinessCenter                       = source.BusinessCenter,
  target.CheckInTime                          = source.CheckInTime,
  target.CheckOutTime                         = source.CheckOutTime,
  target.ClientSpecificPropertyContact        = source.ClientSpecificPropertyContact,
  target.ClientSpecificPropertyTitle          = source.ClientSpecificPropertyTitle,
  target.ConciergeOrTourDesk                  = source.ConciergeOrTourDesk,
  target.ExtendedStayOrResidentialApartment   = source.ExtendedStayOrResidentialApartment,
  target.FullOrLimitedService                 = source.FullOrLimitedService,
  target.IDSalesforce                         = source.IDSalesforce,
  target.IMFCurrencyCode                      = source.IMFCurrencyCode,
  target.MailingAddress1                      = source.MailingAddress1,
  target.MailingAddress2                      = source.MailingAddress2,
  target.MailingCity                          = source.MailingCity,
  target.MailingCountryName                   = source.MailingCountryName,
  target.MailingPostalCode                    = source.MailingPostalCode,
  target.MailingStateOrProvinceCode           = source.MailingStateOrProvinceCode,
  target.Minimumnightstay                     = source.Minimumnightstay,
  target.NearestAirportCode                   = source.NearestAirportCode,
  target.OccupancyTax                         = source.OccupancyTax,
  target.OfficalRatingAgency                  = source.OfficalRatingAgency,
  target.OfficalRatingAgencyOther             = source.OfficalRatingAgencyOther,
  target.PetFriendly                          = source.PetFriendly,
  target.PriceRange                           = source.PriceRange,
  target.PropertyCode                         = source.PropertyCode,
  target.PropertyDescription                  = source.PropertyDescription,
  target.RFPRepresentationCompany             = source.RFPRepresentationCompany,
  target.ResortFeeIncludes                    = source.ResortFeeIncludes,
  target.ReservationEmail1                    = source.ReservationEmail1,
  target.ReservationsPhoneNumber              = source.ReservationsPhoneNumber,
  target.Taxes                                = source.Taxes,
  target.TotalNumberFloors                    = source.TotalNumberFloors,
  target.TotalNumberRoomsOrSuites             = source.TotalNumberRoomsOrSuites,
  target.YearOfLastRenovation                 = source.YearOfLastRenovation,
  target.YearPropertyBuilt                    = source.YearPropertyBuilt,
  target.SELECTHotelFlag                      = source.SELECTHotelFlag,
  target.CuratedHotelFlag                     = source.CuratedHotelFlag,
  target.CityTax                              = source.CityTax,
  target.CityTaxPercentFixed                  = source.CityTaxPercentFixed,
  target.OccupancyTaxinNegotiatedRate         = source.OccupancyTaxinNegotiatedRate,
  target.OccupancyTaxPercentOrFixed           = source.OccupancyTaxPercentOrFixed,
  target.ResortFee                            = source.ResortFee,
  target.ParentChainCode                      = source.ParentChainCode,
  target.SixDigitPhoneNumber                  = source.SixDigitPhoneNumber,
  target.SourceSystem                         = source.SourceSystem,
  target.lastchgusername                      = current_user(),
  target.lastchgdtm                           = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
  Code, Name, HotelName, BrandName, BrandCode, Address1, Address2, City, StateOrProvince, CountryName, CountryCode, PostalCode,
  StreetAddressFullAddressLine, MainPhoneNumber, Website, MarketTier, AmadeusBrandCode, AmadeusPropertyCode,
  ApolloOrGalileoBrandCode, ApolloOrGalileoPropertyCode, SabreBrandCode, SabrePropertyCode, WorldspanBrandCode, WorldspanPropertyCode,
  Latitude, Longitude, StarRating, ParentChainName, FrontDeskPhoneNumber, GeneralManagerPhoneNumber, ArrivalDepartureProtocols,
  BillingAddress1, BillingCity, BillingCountryName, BillingPostalCode, BillingStateOrProvince, BusinessCenter, CheckInTime, CheckOutTime,
  ClientSpecificPropertyContact, ClientSpecificPropertyTitle, ConciergeOrTourDesk, ExtendedStayOrResidentialApartment, FullOrLimitedService,
  IDSalesforce, IMFCurrencyCode, MailingAddress1, MailingAddress2, MailingCity, MailingCountryName, MailingPostalCode,
  MailingStateOrProvinceCode, Minimumnightstay, NearestAirportCode, OccupancyTax, OfficalRatingAgency, OfficalRatingAgencyOther,
  PetFriendly, PriceRange, PropertyCode, PropertyDescription, RFPRepresentationCompany, ResortFeeIncludes, ReservationEmail1,
  ReservationsPhoneNumber, Taxes, TotalNumberFloors, TotalNumberRoomsOrSuites, YearOfLastRenovation, YearPropertyBuilt,
  SELECTHotelFlag, CuratedHotelFlag, CityTax, CityTaxPercentFixed, OccupancyTaxinNegotiatedRate, OccupancyTaxPercentOrFixed, ResortFee,
  ParentChainCode, SixDigitPhoneNumber, SourceSystem, lastchgusername, lastchgdtm, enterusername, enterdtm
)
VALUES (
  source.Code, source.Name, source.HotelName, source.BrandName, source.BrandCode, source.Address1, source.Address2, source.City, source.StateOrProvince,
  source.CountryName, source.CountryCode, source.PostalCode, source.StreetAddressFullAddressLine,
  source.MainPhoneNumber, source.Website, source.MarketTier, source.AmadeusBrandCode, source.AmadeusPropertyCode,
  source.ApolloOrGalileoBrandCode, source.ApolloOrGalileoPropertyCode, source.SabreBrandCode, source.SabrePropertyCode,
  source.WorldspanBrandCode, source.WorldspanPropertyCode, source.Latitude, source.Longitude, source.StarRating, source.ParentChainName,
  source.FrontDeskPhoneNumber, source.GeneralManagerPhoneNumber, source.ArrivalDepartureProtocols, source.BillingAddress1,
  source.BillingCity, source.BillingCountryName, source.BillingPostalCode, source.BillingStateOrProvince, source.BusinessCenter,
  source.CheckInTime, source.CheckOutTime, source.ClientSpecificPropertyContact, source.ClientSpecificPropertyTitle,
  source.ConciergeOrTourDesk, source.ExtendedStayOrResidentialApartment, source.FullOrLimitedService, source.IDSalesforce,
  source.IMFCurrencyCode, source.MailingAddress1, source.MailingAddress2, source.MailingCity, source.MailingCountryName,
  source.MailingPostalCode, source.MailingStateOrProvinceCode, source.Minimumnightstay, source.NearestAirportCode, source.OccupancyTax,
  source.OfficalRatingAgency, source.OfficalRatingAgencyOther, source.PetFriendly, source.PriceRange, source.PropertyCode,
  source.PropertyDescription, source.RFPRepresentationCompany, source.ResortFeeIncludes, source.ReservationEmail1,
  source.ReservationsPhoneNumber, source.Taxes, source.TotalNumberFloors, source.TotalNumberRoomsOrSuites, source.YearOfLastRenovation,
  source.YearPropertyBuilt, source.SELECTHotelFlag, source.CuratedHotelFlag, source.CityTax, source.CityTaxPercentFixed,
  source.OccupancyTaxinNegotiatedRate, source.OccupancyTaxPercentOrFixed, source.ResortFee, source.ParentChainCode,
  source.SixDigitPhoneNumber, source.SourceSystem, current_user(), current_timestamp(), current_user(), current_timestamp()
)
""")

# COMMAND ----------

# DBTITLE 1,Format timestamp for Gold table standard
formatted_gold_ts = datetime.strptime(start_ts_utc, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]

# COMMAND ----------

# DBTITLE 1,Generate csv
df_gold_hotelmdm = filter_hotel_master_df(spark, "Salesforce", formatted_gold_ts)

# COMMAND ----------

columns_to_select = [
    "Code",
    "Name",
    "HotelName",
    "BrandName",
    "BrandCode",
    "Address1",
    "Address2",
    "City",
    "StateOrProvince",
    "CountryCode",
    "CountryName",
    "PostalCode",
    "StreetAddressFullAddressLine",
    "MainPhoneNumber",
    "Website",
    "MarketTier",
    "AmadeusBrandCode",
    "AmadeusPropertyCode",
    "ApolloOrGalileoBrandCode",
    "ApolloOrGalileoPropertyCode",
    "SabreBrandCode",
    "SabrePropertyCode",
    "WorldspanBrandCode",
    "WorldspanPropertyCode",
    "Latitude",
    "Longitude",
    "StarRating",
    "ParentChainName",
    "FrontDeskPhoneNumber",
    "GeneralManagerPhoneNumber",
    "ArrivalDepartureProtocols",
    "BillingAddress1",
    "BillingCity",
    "BillingCountryName",
    "BillingPostalCode",
    "BillingStateOrProvince",
    "BusinessCenter",
    "CheckInTime",
    "CheckOutTime",
    "ClientSpecificPropertyContact",
    "ClientSpecificPropertyTitle",
    "ConciergeOrTourDesk",
    "ExtendedStayOrResidentialApartment",
    "FullOrLimitedService",
    "IDSalesforce",
    "IMFCurrencyCode",
    "MailingAddress1",
    "MailingAddress2",
    "MailingCity",
    "MailingCountryName",
    "MailingPostalCode",
    "MailingStateOrProvinceCode",
    "Minimumnightstay",
    "NearestAirportCode",
    "OccupancyTax",
    "OfficalRatingAgency",
    "OfficalRatingAgencyOther",
    "PetFriendly",
    "PriceRange",
    "PropertyCode",
    "PropertyDescription",
    "RFPRepresentationCompany",
    "ResortFeeIncludes",
    "ReservationEmail1",
    "ReservationsPhoneNumber",
    "Taxes",
    "TotalNumberFloors",
    "TotalNumberRoomsOrSuites",
    "YearOfLastRenovation",
    "YearPropertyBuilt",
    "SELECTHotelFlag",
    "CuratedHotelFlag",
    "CityTax",
    "CityTaxPercentFixed",
    "OccupancyTaxinNegotiatedRate",
    "OccupancyTaxPercentOrFixed",
    "ResortFee",
    "ParentChainCode",
    "SixDigitPhoneNumber",
    "SourceSystem"
]

# COMMAND ----------

df_csv = df_gold_hotelmdm.selectExpr(*columns_to_select)
df_csv = df_csv.fillna('')

# COMMAND ----------

# DBTITLE 1,Export csv
# export data to blob
export_file_name = "hotelmaster_mdm_salesforce"
export_file_name_code = "hotelmaster_mdm_salesforce_codes"

#Get only code field for csv export
df_csv_code = df_csv.select("code")

#Generate files
export_to_csv(spark, df_csv, "/mnt/hotelmaster", f"{export_file_name}.csv")
export_to_csv(spark, df_csv_code, "/mnt/hotelmaster", f"{export_file_name_code}.csv")
