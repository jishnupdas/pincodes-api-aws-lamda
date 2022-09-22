import logging
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_csv(file="data.csv"):
    """
    :return: A pandas dataframe containing the data from the CSV.
    """
    df = pd.read_csv(file)
    return df


def get_pincode(pincode):
    """
    :param pincode: The pincode to search for.
    :return: The result of the search.
    """
    df = parse_csv()
    # convert pincode to integer
    try:
        pincode = int(pincode)
        logger.info(f"Converted pincode to integer: {pincode}")
        return df[df["pincode"] == pincode].to_dict(orient="records")
    except ValueError:
        return "Invalid pincode"


def lambda_handler(event, context):
    """
    :param event: The event dict that contains the parameters sent when the function is invoked.
    :param context: The context in which the function is called.
    :return: The result of the action.
    """
    action = event.get("pincode", None)
    logger.info(f"Given pincode: {action}")

    if action is None:
        logger.error("No pincode given.")
        return {"message": "error", "result": "Invalid action"}

    results = get_pincode(action)
    logger.info(f"Results: {results}")

    response = {"message": "success", "result": results}
    return response
