# 走 DaoCloud 国内镜像源拉基础镜像，避开 Docker Hub 限流
FROM docker.m.daocloud.io/library/python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai

COPY requirements.txt .
RUN pip install --no-cache-dir \
        -i https://pypi.tuna.tsinghua.edu.cn/simple \
        -r requirements.txt

COPY stream_proxy.py .
COPY static ./static

# 持久化目录（机巢列表 nests.json）。docker-compose 用卷挂载该目录
RUN mkdir -p /app/data

EXPOSE 9000

CMD ["python", "stream_proxy.py"]
