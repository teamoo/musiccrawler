# Automatically created by: scrapy deploy

from setuptools import setup, find_packages

setup(
    name='musiccrawler',
    version='1.0',
    packages=find_packages(),
    package_data={'musiccrawler.config': ['*.json','*.cfg']},
    entry_points={'scrapy': ['settings = musiccrawler.settings']},
    zip_safe=False
)
