import logging
import json
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_csv(file="data.csv"):
    """
    :return: A pandas dataframe containing the data from the CSV.
    """
    df = pd.read_csv(file)
    logger.info(f"Read CSV file: {file}")
    return df


def get_pincode(df, pincode):
    """
    :param pincode: The pincode to search for.
    :return: The result of the search.
    """
    # convert pincode to integer
    try:
        pincode = int(pincode)
        logger.info(f"Converted pincode to integer: {pincode}")
        return df[df["pincode"] == pincode].to_dict(orient="records")
    except ValueError:
        logger.error(f"Invalid pincode: {pincode}")
        return "Invalid pincode"


def get_city(df, city):
    """
    :param city: The city to search for.
    :return: The result of the search.
    """
    try:
        return df[df["city"] == city].to_dict(orient="records")
    except ValueError:
        logger.error(f"Invalid city: {city}")
        return "Invalid city"


def filter_df(df, column, value):
    """
    :param df: The dataframe to filter.
    :param column: The column to filter on.
    :param value: The value to filter on.
    :return: The filtered dataframe.
    """
    logger.info(f"Filtering dataframe on column: {column} and value: {value}")
    return df.query(f'{column} == "{value}"').to_dict(orient="records")


def get_results(event):
    """
    :param event: The event dict that contains the parameters sent when the function is invoked.
    :return: The result of the action.
    """
    response = {}
    df = parse_csv()
    for key, value in event.items():
        if key in df.columns:
            response[key] = filter_df(df, key, value)
        else:
            response[key] = "Invalid key"

    return response


def lambda_handler(event, context):
    """
    :param event: The event dict that contains the parameters sent when the function is invoked.
    :param context: The context in which the function is called.
    :return: The result of the action.
    """

    request_data = event.get("queryStringParameters", {})
    logger.info(f"Request data: {request_data}")
    logger.info(f"Event: {event}")
    querydict = dict(event, **request_data)
    logger.info(f"Querydict: {querydict}")

    # check for pincode/city in event and call appropriate function
    return {
        "statusCode": 200,
        "body": json.dumps(get_results(querydict)),
    }
