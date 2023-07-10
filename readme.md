# Usage for Template

## Compile Commands

``` bash
mvn clean install
```

## Run Commands

### Local Single Node Mode

PageRank
``` bash
path/to/hadoop jar target/Final.jar PR -i input -o output -r repeat-cnt
```

LabelPropagation
``` bash
path/to/hadoop jar target/Final.jar LP input output itertimes
```
