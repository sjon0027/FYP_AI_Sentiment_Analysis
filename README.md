# FIT4702 FYP 2025
AI-Driven Sentiment Analysis for Public Perception of Emerging Technologies (technical)

## Automated YouTube and Twitter comment analysis using Selenium and Tweepy

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

#### Update the .env file and place it in the same folder as the app for it to work.

```
OPENROUTER_API_KEY=<retrieve from Open Router>
OPENROUTER_HTTP_REFERER="http://localhost"
OPENROUTER_X_TITLE="Social Media Sentiment Analysis Tool"
OPENROUTER_RPM=120
LLM_BATCH=6
LLM_TEXT_MAXCHARS=200
LLM_MAX_PROMPT_CHARS=120000
LLM_OUT_TOKENS_PER_ROW=14
OPENROUTER_MAX_TOKENS_HARD=16000
X_BEARER_TOKEN=<retrieve from X Developer Portal>
X_CONSUMER_KEY=<retrieve from X Developer Portal>
X_CONSUMER_SECRET=<retrieve from X Developer Portal>
X_ACCESS_TOKEN=<retrieve from X Developer Portal>
X_ACCESS_TOKEN_SECRET=<retrieve from X Developer Portal>
YCS_UI="1"
CHROME_BINARY=<Optional direct link to Chrome for Testing browser file>
CHROMEDRIVER=<Optional direct link to Chrome driver file>
HEADLESS=0
CFT_VERSION=142
X_SEARCH_SCOPE=recent
```

#### Generate a python virtual environment using python version 3.12

`py -3.12 -m venv .venv`

#### Activate the python virtual environment (Windows 11 64 bit machines only)

`.venv\Scripts\activate`

#### Install the required dependencies

`pip install -r requirements.txt --upgrade`

#### Then, run the app like so.

`streamlit run sentiment_analysis_tool.py`

##### Data Acknowledgements

###### The following links have been data mined for this project (YouTube link & Twitter/X search query)

```https://www.youtube.com/watch?v=N9LKQ9tVT28```

```"""AI"" ""Australia"" lang:en -is:retweet"```