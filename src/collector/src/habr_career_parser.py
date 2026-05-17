import json
import time
from collections.abc import Iterator
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from src import log
from src.config import settings
from src.schema import Company, Salary, Vacancy


class HabrCareerParser:
    SOURCE = "habr_career"

    def __init__(self) -> None:
        self._base_url = settings.HABR_CAREER_BASE_URL.rstrip("/")
        self._user_agent = settings.USER_AGENT
        self._delay = settings.HABR_CAREER_REQUEST_DELAY_SECONDS
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self._user_agent})

    def iter_vacancies(self, max_pages: int | None = None) -> Iterator[Vacancy]:
        for link in self.collect_all_links(max_pages=max_pages):
            yield self.parse_vacancy(link)
            time.sleep(self._delay)

    def collect_all_links(self, max_pages: int | None = None) -> list[str]:
        all_links: list[str] = []
        seen: set[str] = set()
        page = 1

        log.info("Start collecting Habr Career links", max_pages=max_pages)

        while True:
            log.info("Fetching vacancies list page", page=page)

            soup = self._get_list_page_soup(page)
            page_links = self._extract_links_from_list_page(soup)

            new_links = []
            for link in page_links:
                if link not in seen:
                    seen.add(link)
                    new_links.append(link)
                    all_links.append(link)

            log.info(
                "Collected links from page",
                page=page,
                page_links=len(page_links),
                new_links=len(new_links),
                total_links=len(all_links),
            )

            if not new_links:
                log.info("Stop collecting links: no new links", page=page)
                break

            if max_pages and page >= max_pages:
                log.info("Stop collecting links: max_pages reached", page=page)
                break

            if not self._has_next_page(soup):
                log.info("Stop collecting links: next page not found", page=page)
                break

            page += 1
            time.sleep(self._delay)

        log.info("Finished collecting links", total_links=len(all_links))
        return all_links

    def parse_vacancy(self, url: str) -> Vacancy:
        log.info("Parsing vacancy", url=url)
        soup = self._get_soup(url)

        jobposting = self._parse_jobposting(soup)
        ssr = self._parse_ssr_state(soup)

        vacancy = ssr.get("vacancy", {})
        company_state = ssr.get("company", {})
        hiring_org = jobposting.get("hiringOrganization", {})

        vacancy_id = str(
            vacancy.get("id")
            or jobposting.get("identifier", {}).get("value")
            or url.rstrip("/").split("/")[-1]
        )

        description_html = vacancy.get("description") or jobposting.get("description")

        parsed = Vacancy(
            source=self.SOURCE,
            id=vacancy_id,
            url=url,
            title=vacancy.get("title") or jobposting.get("title") or "",
            company=Company(
                id=(vacancy.get("company") or {}).get("id"),
                name=company_state.get("name") or hiring_org.get("name"),
                url=urljoin(self._base_url, company_state.get("href", ""))
                if company_state.get("href")
                else None,
                site=(company_state.get("url") or {}).get("href") or hiring_org.get("sameAs"),
            ),
            date_posted=jobposting.get("datePosted"),
            published_at=(vacancy.get("publishedDate") or {}).get("date"),
            published_title=(vacancy.get("publishedDate") or {}).get("title"),
            valid_through=jobposting.get("validThrough"),
            employment_type_schema=jobposting.get("employmentType"),
            employment=vacancy.get("employment"),
            employment_type_text=vacancy.get("employmentType"),
            remote=vacancy.get("remoteWork"),
            job_location_type=jobposting.get("jobLocationType"),
            locations=self._extract_locations(jobposting),
            human_city_names=vacancy.get("humanCityNames"),
            short_geo=vacancy.get("shortGeo"),
            qualification=vacancy.get("qualification"),
            salary_qualification=(vacancy.get("salaryQualification") or {}).get("title"),
            specializations=[
                item.get("title") for item in vacancy.get("divisions", []) if item.get("title")
            ],
            skills=[item.get("title") for item in vacancy.get("skills", []) if item.get("title")],
            salary=self._normalize_salary(jobposting, vacancy),
            description_html=description_html,
            description_text=self._clean_html(description_html),
            banner_description=vacancy.get("bannerDescription"),
        )

        log.info(
            "Parsed vacancy",
            vacancy_id=parsed.id,
            title=parsed.title,
            company=parsed.company.name,
            salary=parsed.salary.formatted,
            skills_count=len(parsed.skills),
            locations=parsed.locations,
        )

        return parsed

    def _get_list_page_soup(self, page: int) -> BeautifulSoup:
        params = {
            "type": "all",
            "sort": "date",
            "page": page,
        }
        url = f"{self._base_url}/vacancies?{urlencode(params)}"
        return self._get_soup(url)

    def _get_soup(self, url: str) -> BeautifulSoup:
        response = self._session.get(url, timeout=20)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_links_from_list_page(self, soup: BeautifulSoup) -> list[str]:
        links = []

        for a in soup.select('a.vacancy-card__title-link[href^="/vacancies/"]'):
            links.append(urljoin(self._base_url, a["href"]))

        return list(dict.fromkeys(links))

    @staticmethod
    def _has_next_page(soup: BeautifulSoup) -> bool:
        return soup.select_one("a.next_page[href]") is not None

    @staticmethod
    def _parse_jobposting(soup: BeautifulSoup) -> dict:
        tag = soup.select_one('script[type="application/ld+json"]')
        if not tag or not tag.string:
            return {}

        return json.loads(tag.string)

    @staticmethod
    def _parse_ssr_state(soup: BeautifulSoup) -> dict:
        tag = soup.select_one('script[type="application/json"][data-ssr-state="true"]')
        if not tag or not tag.string:
            return {}

        return json.loads(tag.string)

    @staticmethod
    def _clean_html(html: str | None) -> str | None:
        if not html:
            return None

        return BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)

    @classmethod
    def _extract_locations(cls, jobposting: dict) -> list[str]:
        job_location = jobposting.get("jobLocation")

        if isinstance(job_location, list):
            locations = []
            for item in job_location:
                location = cls._normalize_location_item(item)
                if location:
                    locations.append(location)
            return locations

        location = cls._normalize_location_item(job_location)
        return [location] if location else []

    @classmethod
    def _normalize_location_item(cls, item: object) -> str | None:
        if not isinstance(item, dict):
            return None

        address = item.get("address")

        if isinstance(address, str):
            return address

        if isinstance(address, dict):
            parts = [
                address.get("addressLocality"),
                address.get("addressRegion"),
                address.get("addressCountry"),
            ]

            normalized_parts = []
            for part in parts:
                if isinstance(part, str):
                    normalized_parts.append(part)
                elif isinstance(part, dict):
                    name = part.get("name")
                    if name:
                        normalized_parts.append(name)

            return ", ".join(normalized_parts) or None

        name = item.get("name")
        if isinstance(name, str):
            return name

        return None

    @staticmethod
    def _normalize_salary(jobposting: dict, vacancy: dict) -> Salary:
        salary = vacancy.get("salary")

        if salary:
            return Salary(
                salary_from=salary.get("from"),
                salary_to=salary.get("to"),
                currency=salary.get("currency"),
                formatted=salary.get("formatted"),
                period="MONTH",
            )

        base_salary = jobposting.get("baseSalary")
        if base_salary:
            value = base_salary.get("value", {})
            return Salary(
                salary_from=value.get("minValue") or value.get("value"),
                salary_to=value.get("maxValue"),
                currency=base_salary.get("currency"),
                formatted=None,
                period=value.get("unitText"),
            )

        return Salary()
