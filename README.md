# Crude data preparation for an InfluxDB based, germany-focused COVID-19 dashboard

## Into the deep end

(Sorry for not spending the time to write clean and thorough documentation.)

**Note:** You need influx at ``http://localhost:8086``. It needs to have a database called `covid` where everything will end up. Read the python scripts for details.

Do a full initialisation of the database:

```console
$ ./update-daily.sh
$ ./update-monthly.sh
```

This may take a while. In addition, when it displays percentages, those are *very* rough and it is expected that the process completes *way* before reaching 100%. Be glad about that, otherwise it’d take ages.

### Updating the database

- RKI and JHU roughly update at 00:00 UTC, so you can run it whenever in the morning.
- DWD updates at strange intervals, you’ll have to check. Patches which extend the download shellscripts to assess whether data is already there before downloading to be nicer to DWD servers gladly accepted.
- The `*-to-influx.py` scripts are designed so that you can safely re-execute them against a filled database without the data going bad. An exception is that when *all* numbers for a day drop to zero (unlikely) in a new release of the data, that will not be reflected in the DB because we don’t send those samples to save processing capacity.
