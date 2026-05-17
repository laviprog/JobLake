from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class Salary(BaseModel):
    salary_from: Optional[float] = None
    salary_to: Optional[float] = None
    currency: Optional[str] = None
    formatted: Optional[str] = None
    period: Optional[str] = None


class Company(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    url: Optional[str] = None
    site: Optional[str] = None


class Vacancy(BaseModel):
    source: str

    id: str
    url: str

    title: str
    company: Company

    date_posted: Optional[date] = None
    published_at: Optional[datetime] = None
    published_title: Optional[str] = None
    valid_through: Optional[date] = None

    employment_type_schema: Optional[str] = None
    employment: Optional[str] = None
    employment_type_text: Optional[str] = None

    remote: Optional[bool] = None
    job_location_type: Optional[str] = None
    locations: list[str] = []
    human_city_names: Optional[str] = None
    short_geo: Optional[str] = None

    qualification: Optional[str] = None
    salary_qualification: Optional[str] = None

    specializations: list[str] = []
    skills: list[str] = []

    salary: Salary = Salary()

    # content
    description_html: Optional[str] = None
    description_text: Optional[str] = None
    banner_description: Optional[str] = None
