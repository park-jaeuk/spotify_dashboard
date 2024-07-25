FROM apache/airflow:2.7.3-python3.9

USER root

RUN apt-get update && apt-get install -y wget unzip xclip

# Chrome .deb 패키지를 이용해서 원하는 버전 다운로드
RUN wget "http://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.90-1_amd64.deb" -O /tmp/chrome.deb \
    && dpkg -i /tmp/chrome.deb || apt-get install -fy

# 설치 후 불필요한 파일 정리
RUN rm /tmp/chrome.deb

# ChromeDriver 다운로드 및 설치
RUN wget "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip" \
    && unzip chromedriver_linux64.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/chromedriver \
    && rm chromedriver_linux64.zip

USER airflow

ADD  requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt