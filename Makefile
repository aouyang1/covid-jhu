install:
	pip3 install -r requirements.txt

run:
	python3 scraper.py
	python3 study.py

init_db:
	mysql -u root -p -D covid < init.sql
