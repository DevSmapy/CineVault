from requests import get
from typing import Any, List, Dict, Optional

from pyre_extensions import ParameterSpecification

P = ParameterSpecification("P")


def generate_params(*_: P.args, **kwargs: P.kwargs) -> Dict[str, Any]:
    params = {}
    params.update(kwargs)
    return params


def fetch_data_from_api(url: str, params: Dict[str, Any]) -> Optional[str]:
    response = get(url, params=params)
    if response.status_code == 200:
        return response.text


def str2dict(data: str) -> Dict[str, Any]:
    return eval(data)


def fetch_multiple_pages(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    page = 1

    while True:
        params["curPage"] = page  # current page number
        response = fetch_data_from_api(url, params)
        data = str2dict(response)
        if data["movieListResult"]["totCnt"] == 0:  # No more data
            break
        result.extend(data["movieListResult"]["movieList"])
        page += 1
    return result
