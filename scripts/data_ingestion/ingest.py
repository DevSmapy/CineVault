from requests import get
from typing import Any, Dict, Optional

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
