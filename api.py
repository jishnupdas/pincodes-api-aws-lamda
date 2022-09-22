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


def filter_df(df, column, value):
    """
    :param df: The dataframe to filter.
    :param column: The column to filter on.
    :param value: The value to filter on.
    :return: The filtered dataframe.
    """
    logger.info(f"Filtering dataframe on column: {column} and value: {value}")
    return df.query(f"{column} == {value}").to_dict(orient="records")


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

    if response.keys():
        return response
    else:
        return {"message": "No results found"}


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
