# This makefile will test the expected against observed output.
# If the make command fails, then there is a problem

all:
	d79 load_gis gisaid.xls > .obs-gisaid.ttl
	diff .obs-gisaid.ttl .exp-gisaid.ttl
	rm .obs*
	# ensure no usage requests fail
	bash help.sh > /dev/null
