CREATE TABLE covid (
  report_date DATE,
  city VARCHAR(80),
  province_state VARCHAR(50),
  country VARCHAR(50),
  confirmed INT,
  deaths INT,
  recovered INT,
  active INT,
  daily_confirmed INT,
  daily_deaths INT,
  daily_recovered INT,
  daily_active INT,
  PRIMARY KEY(report_date, city, province_state, country)
);

CREATE INDEX report_date_idx on covid (report_date);
CREATE INDEX city_idx on covid (city);
CREATE INDEX province_state_idx on covid (province_state);
CREATE INDEX country_idx on covid (country);

CREATE TABLE covid_region (
  day DATE,
  region VARCHAR(80),
  cases INT,
  deaths INT,
  daily_cases INT,
  daily_deaths INT,
  PRIMARY KEY(day, region)
);

CREATE INDEX day_idx on covid_region (day);
CREATE INDEX region_idx on covid_region (region);

