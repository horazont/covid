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
