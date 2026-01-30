# Databricks notebook source
# MAGIC %run ../mdm_config

# COMMAND ----------

entity= dbutils.widgets.get("entity")
start_timestamp= dbutils.widgets.get("start_timestamp")
end_timestamp= dbutils.widgets.get("end_timestamp")
time_offset_minutes = dbutils.widgets.get("time_offset_minutes")
if time_offset_minutes and time_offset_minutes.strip() and not isinstance(time_offset_minutes, int):
    time_offset_minutes = int(time_offset_minutes)
else:
    time_offset_minutes = None
print(entity)
print(start_timestamp)
print(end_timestamp)
print(time_offset_minutes)

# COMMAND ----------

VENDOR_MASTER = "VendorMaster"
HOTEL_MASTER = "HotelMaster"
HOTEL_CHAIN_MASTER = "HotelChain"

logging.basicConfig(level=logging.DEBUG)
logging = logging.getLogger(__name__)

# Entity Mappings
ENTITY_MAP = {
    HOTEL_MASTER: {
        "match_strategy": "Matching_Survivorship_HotelMaster",
    },
    VENDOR_MASTER: {
        "match_strategy": "Matching_Survivorship_VendorMaster",
    },
}

# List of SourceSystem to exclude
excluded_sources = ['MDM', 'BWFile', 'OnyxFile', 'IHGFile']

# Application Error Code
APP_ERROR_CODE = 900
# Maximum Attempts for API Call
MAX_ATTEMPTS = 10
# Backoff Milli Seconds
BACKOFF_MILLI_SECONDS = 100
# API Retry Delay Milli Seconds
API_RETRY_DELAY_MILLI_SECONDS = 1000
# Fail Code returned on API Calls after retries exhausted
RETRY_FAIL_CODE = 900

class ApplicationException(Exception):
    def __init__(self, message, error_code, detailed_message=None):
        super().__init__(message)
        self.error_code = error_code
        self.detailed_message = detailed_message


def parse_timestamp(timestamp_str):
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',  # Format with milliseconds
        '%Y-%m-%dT%H:%M:%S.%f',   # Format with milliseconds without 'Z'
        '%Y-%m-%dT%H:%M:%SZ',     # Format without milliseconds with 'Z'
        '%Y-%m-%dT%H:%M:%S',      # Format without milliseconds
    ]
    for fmt in formats:
        try:
            return datetime.datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{timestamp_str}' does not match any known formats.")


def can_skip_match_and_survive(client, entity, record_code, start_dt_utc, end_dt_utc, time_offset_minutes):
    client.displayResults = False
    (status, txn_response) = client.list_transactions(admin_x_api_key, entity, record_code)
    client.displayResults = False
    if "totalRecords" not in txn_response or txn_response.get("totalRecords") == 0:
        logging.info("No transaction records present; skipping")
        return True
    if "data" not in txn_response or len(txn_response.get("data")) == 0:
        logging.info("No transaction data records found; skipping")
        return True
    start_dt_txn = start_dt_utc - timedelta(minutes=time_offset_minutes)
    logging.info(f"The new offset starttime is {start_dt_txn } and the original starttime was {start_dt_utc} ")
    txn_records = txn_response.get("data")
    for txn in txn_records:
        txn_id = txn.get("id")
        logging.info(f"\tProcessing transaction {txn_id}")
        comparison_timestamp = txn.get("transactionDTM")
        comparison_dt = parse_timestamp(comparison_timestamp).replace(tzinfo=timezone.utc)
        if comparison_dt < start_dt_txn:
            # Transactions are reverse ordered by timestamp and once start period expires can return as skip
            logging.info(f"\tTransaction date {comparison_dt} is before new offset starttime {start_dt_txn}; skipping")
            return True
        if start_dt_txn <= comparison_dt <= end_dt_utc:
            before_atts = txn.get("beforeMemberAttributes")
            after_atts = txn.get("afterMemberAttributes")
            if not before_atts and not after_atts:
                logging.info("\tNothing changed in the transaction record; skipping")
                continue
            else:
                logging.info("\tTransaction has changes; processing this")
                # Checking if this is only the matching related changes
                expected_keys = {'modelConfidence', 'matchStatus', 'matchMember', 'matchStrategy', 'matchMultiGroup'}
                logging.info(f"\tSet After attributes are {set(after_atts.keys())}")
                if set(after_atts.keys()) == expected_keys:
                    logging.info("\tTransaction has only matching attributes; skipping this transaction")
                    return True
                logging.info("\tTransaction has candidate changes; processing this transaction")
                return False
        else:
            logging.info(f"\tTransaction date {comparison_dt} is not within the start {start_dt_utc} and end {end_dt_utc} timestamps; skipping")
    return True


def process_changed_source_records(client, entity, start_timestamp, end_timestamp, time_offset_minutes) -> dict:
    start_dt_utc = parse_timestamp(start_timestamp).replace(tzinfo=timezone.utc)
    start_str = start_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_dt_utc = parse_timestamp(end_timestamp).replace(tzinfo=timezone.utc)
    end_str = end_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    exclusion_filter = ' and '.join(
        f"[SourceSystem] ne '{src}'" for src in excluded_sources
    )
    gr_filter = (
        f"{exclusion_filter} and "
        f"[LastChgDTM] ge {start_str} and [LastChgDTM] lt {end_str}"
    )
    logging.info(f"Using query filter {gr_filter}")
    page_number = 1
    processed_records = 0
    skipped_records = 0
    while True:
        try:
            client.displayResults = False
            (status, result) = client.query_record(admin_x_api_key, entity, gr_filter, page_number=page_number)
            client.displayResults = True
            if not result or result.get("totalRecords", 0) < 1:
                break
            data = result.get("data")
            if data is None or len(data) < 1:
                break
            total_records = result.get("totalRecords", 0)
            page_size = result.get("pageSize", 0)
            for index, rec in enumerate(data):
                try:
                    current_index = page_size * (page_number - 1) + (index + 1)
                    record_code = rec["code"]
                    logging.warning(
                        f"******* Processing ({current_index} of {total_records}, {round(current_index/total_records*100)}% complete) *******"
                    )
                    logging.info(f">>>>>>> In page {page_number} index {index} code [{record_code}] >>>>>>>")
                    # Optimization steps; under construction
                    if can_skip_match_and_survive(client, entity, record_code, start_dt_utc, end_dt_utc, time_offset_minutes):
                        logging.info(f"Skipping match and survive for record {record_code}")
                        skipped_records += 1
                        logging.info(f"======= Processed {processed_records} and skipped {skipped_records} =======")
                        continue
                    processed_records += 1
                    logging.warning(f"======= Processed {processed_records} and skipped {skipped_records} =======")
                    # Now match and survive the record
                    (status, match_response) = client.match_record(admin_x_api_key, ENTITY_MAP[entity]["match_strategy"], record_code)
                    logging.info(
                        f"Matched source record {status} with match response {match_response}"
                    )
                    match_cluster = match_response.get("matchCluster", "")
                    if not match_cluster or match_cluster == "":
                        logging.info(
                            f"Matched cluster not found for {record_code} - skipping record from processing"
                        )
                        # Matching ran into some problem
                        continue
                    golden_record_id = survive_cluster(client, entity, match_cluster)
                    logging.info(
                        f"Survived source record with golden record {golden_record_id}"
                    )
                except Exception as e:
                    # Need to revisit record to re-process
                    logging.error(
                        f"Encountered error when processing record {record_code}"
                    )
            if "totalPages" in result and result.get("totalPages", 0) > page_number:
                page_number += 1
            else:
                break
        except Exception as e:
            # Handle other types of exceptions, log and exit
            logging.error(
                f"Encountered error when processing page {page_number}"
            )
    return processed_records, skipped_records

def survive_cluster(client, entity, match_cluster):
    logging.info(f"Processing Survivorship for Matched cluster {match_cluster}")
    (status, survive_response) = client.survive_record(admin_x_api_key, ENTITY_MAP[entity]["match_strategy"], match_cluster)
    logging.info(f"Survivorship response {survive_response}")
    golden_record_id = survive_response.get(f"{match_cluster}", "")
    if golden_record_id == "":
        logging.info("Golden record not created by survivorship")
        # Survivorship ran into some problem
        raise ApplicationException(
            "MDM Record Survivorship Issue",
            APP_ERROR_CODE,
            "Golden record not created by survivorship",
        )
    logging.info(f"Survivorship returned golden record {golden_record_id}")
    return golden_record_id


class MDM_Client:
    def __init__(self):
        self.displayResults = True

    def call_api(
        self, action: str, method: str, url: str, api_key, payload: dict
    ) -> Tuple[int, dict]:
        logging.info(f"Calling Profisee {action} API")
        headers_to_use = {"x-api-key": api_key, "Accept": "application/json"}
        retries = 0
        while retries < MAX_ATTEMPTS:
            try:
                if method == "GET":
                    response = requests.request(
                        "GET", url, headers=headers_to_use, json=payload
                    )
                else:
                    response = requests.request(
                        method, url, headers=headers_to_use, json=payload
                    )
                # response_object = json.loads(response.text)
                response_object = response.text
                if response_object is not None and response_object != "":
                    response_object = response.json()
                if self.displayResults:
                    logging.info(
                        f"Profisee API call {action} {method} returned response {response_object}"
                    )
                return response.status_code, response_object
            except ConnectionResetError as e:
                logging.error(
                    f"ConnectionReset Error; Attempt {retries} failed: {e}. Retrying in {API_RETRY_DELAY_MILLI_SECONDS / 1000.0} seconds..."
                )
                time.sleep(API_RETRY_DELAY_MILLI_SECONDS / 1000.0)
            except Exception as e:
                # Handle other types of exceptions, log and exit
                logging.error(
                    f"General Exception; Attempt {retries} failed: {e}. Retrying in {API_RETRY_DELAY_MILLI_SECONDS / 1000.0} seconds..."
                )
                time.sleep(API_RETRY_DELAY_MILLI_SECONDS / 1000.0)
            retries += 1
        return (RETRY_FAIL_CODE, None)

    def query_record(self, api_key, entity, filter, page_number=1, page_size=50) -> Tuple[int, dict]:
        url = (
            f"{mdm_url}/rest/v1/Records/{entity}"
            + f"?Attributes=&CountsOnly=false&DbaFormat=0&Filter={filter}"
            + f"&OrderBy=&PageNumber={page_number}&PageSize={page_size}&RecordCodes=&RecordUid="
        )
        (status, object) = self.call_api("Query", "GET", url, api_key, None)
        if status != 200:
            logging.error(f"Profisee query_record returned status={status}")
            logging.error(f"Message: {object}")
            raise ApplicationException("MDM Query Error", status, object)
        return (status, object)


    def match_record(self, api_key, match_strategy, recordCode) -> Tuple[int, dict]:
        url = f"{mdm_url}/rest/v1/Matching/{match_strategy}/matches"
        data = {"recordCode": recordCode}
        (status, object) = self.call_api("Match", "PUT", url, api_key, data)
        if status != 200:
            logging.error(f"Profisee match_record returned status={status}")
            logging.error(f"Message: {object}")
            raise ApplicationException("MDM Record Matching Error", status, object)
        return (status, object)

    def survive_record(self, api_key, match_strategy, matchClusterId) -> Tuple[int, dict]:
        url = (
            f"{mdm_url}/rest/v1/Matching/{match_strategy}/"
            + f"survivorship?matchClusterIds={matchClusterId}"
        )
        (status, object) = self.call_api("Survive", "POST", url, api_key, {})
        if status != 200:
            logging.error(f"Profisee survive_record returned status={status}")
            logging.error(f"Message: {object}")
            raise ApplicationException("MDM Record Survivorship Error", status, object)
        return (status, object)

    def list_transactions(self, api_key, entity, recordCode) -> Tuple[int, dict]:
        params = encode_query_params({"recordCode": recordCode})
        url = f"{mdm_url}/rest/v1/Transactions/{entity}?{params}"
        (status, object) = self.call_api(
            "Transactions", "GET", url, api_key, {}
        )
        if status != 200:
            logging.error(f"Profisee list_transactions returned status={status}")
            logging.error(f"Message: {object}")
            raise ApplicationException("MDM Record Matching Error", status, object)
        return (status, object)


def main_process(entity: str, start_timestamp: str, end_timestamp: str, time_offset_minutes: int):
    request_id = str(uuid.uuid4())
    logging.info(f"Request [{request_id}] received for {entity} start {start_timestamp} and end {end_timestamp} offset -{time_offset_minutes} minutes")
    start_time = time.time()
    receipt_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    response = {
        "request_id": request_id,
        "request_time": receipt_time,
        "processed_records": 0,
        "skipped_records": 0,
        "processing_time_in_secs": None,
        "operation_status": None,
        "error": None,
    }
    try:
        # Check the entity type
        if entity not in ENTITY_MAP:
            response["operation_status"] = "ERROR"
            response["error"] = f"failed with invalid entity name: {entity} in path"
            json_data = json.dumps(response)
            logging.error(json_data)
            return json_data
        # Setup the client object
        client = MDM_Client()
        # Process each object in the input list
        logging.info("Calling Process Changed Source Records")
        processed_records, skipped_records = process_changed_source_records(client, entity, start_timestamp, end_timestamp, time_offset_minutes)
        stop_time = time.time()
        total_time = stop_time - start_time
        logging.info(
            f"Request [{request_id}] processing completed in {total_time:.2f} seconds"
        )
        response["processed_records"] = processed_records
        response["skipped_records"] = skipped_records
        response["processing_time_in_secs"] = f"{total_time:.2f}"
        response["operation_status"] = "SUCCESS"
        response["error"] = None
        logging.info(
            f"Request [{request_id}] returning result {response}"
        )
        json_data = json.dumps(response)
        return json_data
    except jsonschema.ValidationError as e:
        logging.error(e)
        response["operation_status"] = "ERROR"
        response["error"] = f"Input validation failed: {e}"
        json_data = json.dumps(response)
        return json_data
    except Exception as e:
        logging.error(e)
        response["operation_status"] = "ERROR"
        response["error"] = f"Request failed with error: {str(e)}"
        json_data = json.dumps(response)
        return json_data



# COMMAND ----------

# if __name__ == '__main__':    
#     entity = sys.argv[1]
#     start_timestamp = sys.argv[2]
#     end_timestamp = sys.argv[3]
#     logging.info(f"Starting process for {entity} with start {start_timestamp} and end {end_timestamp}")
#     main_process(entity, start_timestamp, end_timestamp)

# COMMAND ----------

logging.info(f"Starting process for {entity} with start {start_timestamp} and end {end_timestamp} offset -{time_offset_minutes} minutes")
main_process(entity, start_timestamp, end_timestamp, time_offset_minutes)
