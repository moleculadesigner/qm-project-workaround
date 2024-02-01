import io
import sys
import json
from pathlib import Path
from time import sleep

import requests
from bs4 import BeautifulSoup

import pandas as pd


FORM_URL = "https://cccbdb.nist.gov/getformx.asp"
EXP1_URL = "https://cccbdb.nist.gov/exp1x.asp"
EXP2_URL = "https://cccbdb.nist.gov/exp2x.asp"

def headers(**kwargs) -> dict:
    """
    """

    defaults = {
        "Host": "cccbdb.nist.gov",
        "Connection": "keep-alive",
        "Content-Length": "26",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Origin": "https://cccbdb.nist.gov",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-CA,en-GB;q=0.8,en-US;q=0.6,en;q=0.4",
        #"Referer": referer, ## Depends on query
    }
    defaults.update(kwargs)
    return defaults

def get_exp_data_by_cas(cas: int) -> bytes:
    """
    """

    session = requests.Session()
    payload = {
        "formula": cas,
        "submit1": "Submit",
    }
    query_headers = headers(Referer=EXP1_URL)
    print(f"{cas:>12} :: Submitting Query ...")
    query_res = session.post(
        FORM_URL,
        data=payload,
        headers=query_headers,
        allow_redirects=False,
    )
    if query_res.status_code != 302:
        raise requests.RequestException(
            f"Wrong Query responce {query_res.status_code}: must be 302"
        )

    print(f"{cas:>12} :: Collecting Data ...")
    data_res = session.get(EXP2_URL)
    if data_res.status_code != 200:
        raise requests.RequestException(
            f"Wrong Query responce {data_res.status_code}: must be 200"
        )

    return data_res.content

def decorate_html(tag):
    return io.StringIO(
        "<!DOCTYPE HTML><html><body>"
        f"{tag}"
        "</body></html>"
    )

def extract_info(cas: int, html_content: bytes, data_dir: Path=Path("./")):
    """
    """

    soup = BeautifulSoup(html_content, 'html.parser')

    vibrations_div = soup.find(
        "div",
        attrs={
            "class": "box",
            "title": "Vibrational symmetries, frequencies, and intensities"
        },
    )
    vibrations_table = decorate_html(vibrations_div.find('table'))
    vibs = pd.read_html(vibrations_table)[0]
    vibs.to_csv(
        data_dir/f"{cas}_vibs.csv",
    )

    refs_div = soup.find(
        "div",
        attrs={
            "class": "box",
            "title": "References"
        }
    )
    refs_table = decorate_html(refs_div.find("table", attrs={"id": "reftable"}))   
    refs = pd.read_html(refs_table)[0]
    refs.to_csv(
        data_dir/f"{cas}_refs.csv",
    )

def process_cas(cas: int,
                data_dir: Path=Path("./"),
                n_try: int=5,
                retry_timeout: int=2):
    """
    """

    print(f"{cas:>12} :: Processing CCCBDB ...")
    for try_no in range(n_try):
        try:
            data = get_exp_data_by_cas(cas)
            break
        except Exception as e:
            print(f"{cas:>12} :: try {try_no + 1}")
            print(f"{cas:>12} :: {e}")
            sleep(retry_timeout)
            continue
    data_html = data_dir/f"{cas}.html"
    with data_html.open("bw") as dump:
        dump.write(data)
    print(f"{cas:>12} :: Done\n")

def process_table(dataframe: pd.DataFrame,
                  processed_registry: Path,
                  data_dir: Path=Path("./"),
                  n_try: int=5,
                  retry_timeout: int=2):
    
    with processed_registry.open("r") as proc_reg_json:
        processed_entries = json.load(proc_reg_json)
    if "done" not in processed_entries:
        raise ValueError("Omitted mandatory field 'done' in registry file")
    if "falied" not in processed_entries:
        raise ValueError("Omitted mandatory field 'failed' in registry file")
    
    for cas in dataframe["cas_no"]:
        if cas in processed_entries["done"]:
            continue
        try:
            process_cas(cas, data_dir, n_try, retry_timeout)
            processed_entries["done"].append(cas)
            processed_entries.pop(cas, None)
        except Exception as e:
            processed_entries["falied"][cas] = str(e)
        finally:
            with processed_registry.open("w") as proc_reg_json:
                json.dump(processed_entries, proc_reg_json)

def main():
    _, df_path, reg_path, *rest = sys.argv
    if len(rest) == 1:
        n_try = int(rest[0])
        retry_timeout = 2
    elif len(rest) > 1:
        n_try = int(rest[0])
        retry_timeout = int(rest[1])
    else:
        n_try = 5
        retry_timeout = 2
    df_path = Path(df_path).resolve().absolute()
    reg_path = Path(reg_path)

    dataframe = pd.read_csv(df_path)
    if "cas_no" not in dataframe:
        raise ValueError("No column `cas_no` in the data frame")
    
    if not reg_path.exists():
        with reg_path.open("w") as reg_file:
            json.dump({}, reg_file)
    
    with reg_path.open("r") as reg_file:
        registry = json.load(reg_file)

    if "done" in registry:
        assert isinstance(registry["done"], list)
    else:
        registry["done"] = []
        with reg_path.open("w") as reg_file:
            json.dump(registry, reg_file)
    
    if "falied" in registry:
        assert isinstance(registry["falied"], dict)
    else:
        registry["falied"] = {}
        with reg_path.open("w") as reg_file:
            json.dump(registry, reg_file)
    
    print(f"Processing CAS number from table:\n    {df_path.as_posix()}")
    data_dir = Path("cccbdb_data")
    data_dir.mkdir(parents=True, exist_ok=True)

    process_table(
        dataframe=dataframe,
        processed_registry=reg_path,
        data_dir=data_dir,
        n_try=n_try,
        retry_timeout=retry_timeout
    )

if __name__ == "__main__":
    main()