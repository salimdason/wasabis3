from formatter import indicators
from wasabi import WasabiPurge


def initialize():
    accessKey = input(
        f"{indicators.OKCYAN}Enter your Access Key ID: {indicators.ENDC}"
    ).strip()
    secretAccessKey = input(
        f"{indicators.OKCYAN}Enter your Secret Access Key: {indicators.ENDC}"
    ).strip()
    bucketName = input(
        f"{indicators.OKCYAN}Enter your Bucket Name: {indicators.ENDC}"
    ).strip()
    endpoint = input(
        f"{indicators.OKCYAN}Specify the endpoint URL: {indicators.ENDC}"
    ).strip()

    WasabiClient = WasabiPurge(accessKey, secretAccessKey, endpoint, bucketName)

    return WasabiClient
