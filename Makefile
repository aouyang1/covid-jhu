install:
	pip install -r requirements.txt

run:
	python scraper.py
	python study.py

init_db:
	mysql -u root -p -D covid < init.sql
