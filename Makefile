run_scraper:
	bash -c "source .env && python py_scripts/act_scraper.py"

watch_scrape_outputs:
	bash -c "python py_scripts/scrape_loader.py"
