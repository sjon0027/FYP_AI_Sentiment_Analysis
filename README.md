# ENG4701/FIT4701 FYP 2025
AI-Driven Sentiment Analysis for Public Perception of Emerging Technologies

## Automated YouTube comment scraping using Selenium

### Steps to reproduce

#### Ensure that your chrome version and the chrome driver version is updated to the latest version as mentioned here.

`https://googlechromelabs.github.io/chrome-for-testing/#stable`

#### For the context of this project, we are assuming a 64-bit Windows version for the execution platform. You are required to download the zip then extract the folder as-is from the zip file in order to replace the current version in the event that this version is out-of-date.

`cd .\chrome-extension-files\`

#### Similarly git pull the YCS continued extension within the chrome-extension-files folder for extension accessibility

#### retrieve the chrome extension from their GitHub repository

`git remote add ycs https://github.com/pc035860/YCS-cont.git`
`git fetch ycs`

#### Then pull the 'main' branch as a subtree as "chrome-extension-files"

`git subtree add --prefix=chrome-extension-files ycs main --squash`

#### Then pull the current 'main' branch as a subtree under the existing project root folder

`git subtree pull --prefix=chrome-extension-files ycs main --squash`

#### Then return to current project directory

`cd ..`

#### Create the virtual environment, activate it and install requests, selenium, vaderSentiment and tqdm which are required dependencies for the project to work.

`python -m venv .venv`

`.venv\Scripts\activate`

`pip install selenium vaderSentiment requests tqdm twscrape`

#### Then, run the Automated Selenium Processing script in order to process YouTube videos and output processed JSON files accordingly.

`python sentiment_dashboard_prototype.py <URL 1> [<URL 2> ... etc]`