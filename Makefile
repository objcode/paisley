trial:
	PYTHONPATH=`pwd` trial paisley.test

coverage: paisley
	PYTHONPATH=`pwd` trial --coverage paisley.test
	find _trial_temp/coverage -name 'paisley.*.cover' | grep -v paisley.test | grep -v paisley.mapping | xargs scripts/show-coverage.py

clean:
	rm paisley/*.pyc paisley/test/*.pyc

pep8:
	find paisley -name '*.py' | grep -v paisley/mapping.py | xargs scripts/pep8.py --repeat

check: trial pep8

testkill:
	ps auxw | grep beam | grep paisley | cut -c10-16 | xargs kill

trialall:
	for couchdb in ~/prefix/couchdb/*; do echo $$couchdb; PATH=$$couchdb:$$PATH trial paisley; done
