copy into {{ params.table }} from @{{ params.stage }}/premier_positions.csv.gz FILE_FORMAT=(TYPE=csv field_delimIter=',' skip_header=1) ON_ERROR = 'CONTINUE';

