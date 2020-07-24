# MAKEFILE
MODUEL_NAME:=omi_cache_manager
TEST_CASE_DIR:=`pwd`/test

install:
	python setup.py install

coverage:
	cd ${TEST_CASE_DIR} && \
    pytest --cov=${MODUEL_NAME} --cov-report=html ./test*
    
unittest:
	cd ${TEST_CASE_DIR} && \
    pytest ./test*

echo:
	echo ${MODUEL_NAME}