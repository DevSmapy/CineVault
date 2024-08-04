from scripts.data_ingestion import fetch_data_from_api, generate_params

def main():
    api_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"

    params = generate_params(key="",
                    targetDt="20120101")

    print(fetch_data_from_api(api_url, params=params))
if __name__ == "__main__":
    main()