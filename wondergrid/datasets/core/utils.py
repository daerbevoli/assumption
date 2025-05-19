import os
import requests
import zipfile_deflate64
import humanize


def download(url, filepath) -> str:
    print(f'downloading {url} ...')
    if os.path.exists(filepath):
        print(f'skipped download, file {filepath} already exists')
    else:
        try:
            with requests.get(url, timeout=30.0, stream=True) as response:
                response.raise_for_status()
                with open(filepath, mode='wb') as file:
                    for chunk in response.iter_content(chunk_size=65536):
                        file.write(chunk)
            print(f'saved downloaded file to {filepath} ({humanize.naturalsize(os.path.getsize(filepath))})')
        except requests.exceptions.HTTPError as error:
            print(f'failed download of {url} with http status code: {error.response.status_code}')
            return None
    return filepath


def extract_deflate64_zip(zipfilepath) -> list[str]:
    print(f'extracting {zipfilepath} ...')
    archive = zipfile_deflate64.ZipFile(zipfilepath, 'r')
    filepaths = []
    for fileinfo in archive.infolist():
        filepath = os.path.join(os.path.dirname(zipfilepath), fileinfo.filename)
        if os.path.exists(filepath):
            print(f'skipped extraction, file {filepath} already exists')
        else:
            with open(filepath, mode='wb') as file:
                with archive.open(fileinfo, 'r') as zipfile:
                    while chunk := zipfile.read(65536):
                        file.write(chunk)
            print(f'extracted file {filepath} ({humanize.naturalsize(os.path.getsize(filepath))})')
        filepaths.append(filepath)
    return filepaths
