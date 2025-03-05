from bs4 import BeautifulSoup
from pytest import mark, fixture
from typing import List, Tuple, Optional
from requests import get as _get, Response, session
from concurrent.futures import ThreadPoolExecutor, as_completed

@fixture(scope='session')
def setup():
    open('bad_statuses.txt', 'w').close()
    open('bad_canonical.txt', 'w').close()

@mark.parametrize('index_sitemap_url',['https://www.pachca.com/sitemap.xml'])
def test_p_check_sitemap(index_sitemap_url, setup):

    r = _get(url=index_sitemap_url)
    assert 200 == r.status_code, 'Index sitemap not available'

    has_bad_status, has_bad_canonical = check_urls(r)
    assert not has_bad_status, 'There are STATUS CODES != 200'
    assert not has_bad_canonical, 'There are mismatches between SITEMAP URL and CANONICAL URL'

def get_canonical_url(r: Response) -> Optional[str]:
    soup = BeautifulSoup(r.text, 'html.parser')
    canonical_tag = soup.find("link", rel="canonical")

    if canonical_tag:
        return canonical_tag["href"].strip()
    return None

def check_url(url: str, is_index: bool) -> Tuple[bool, bool]:
    has_bad_status, has_bad_canonical = False, False
    response = _get(url)

    if is_index:
        if response.status_code == 200:
            return check_urls(response)
        else:
            has_bad_status = True
            with open('bad_statuses.txt', 'a') as status_file:
                status_file.write(f"{url} - {response.status_code}\n")
    elif response.status_code != 200:
            has_bad_status = True
            with open('bad_statuses.txt', 'a') as status_file:
                status_file.write(f"{url} - {response.status_code}\n")
    else:
        canonical_url = get_canonical_url(response)
        if canonical_url != url.strip():
            has_bad_canonical = True
            with open('bad_canonical.txt', 'a') as canonical_file:
                canonical_file.write(f"URL: {url} - {canonical_url}\n")
    return has_bad_status, has_bad_canonical

def check_urls(r: Response, max_workers=20) -> Tuple[bool, bool]:
    has_bad_status, has_bad_canonical = False, False
    soup = BeautifulSoup(r.text, 'xml')
    urls = [tag.text.strip() for tag in soup.find_all('loc')]
    is_index = bool(soup.find_all("sitemapindex"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(check_url, url, is_index): url
            for url in urls
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            print(url)
            url_has_bad_status, urs_has_bad_canonical = future.result()
            has_bad_status = has_bad_status or url_has_bad_status
            has_bad_canonical = has_bad_canonical or urs_has_bad_canonical

    return has_bad_status, has_bad_canonical
