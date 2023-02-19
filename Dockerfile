FROM apache/spark-py
USER root
RUN pip install pandas
ENV SOURCE_PATH="/files"
ENV OUT_PATH="/out"
RUN mkdir ${OUT_PATH}
COPY --chmod=777 src /src/
COPY files ${SOURCE_PATH}/
WORKDIR /src
