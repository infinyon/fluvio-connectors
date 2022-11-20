# Common Connectors Utils
The common-utils crate to use when building a fluvio connector in Rust

### Table of Contents
1. [Connector Config](#connector-config)  
1.1. [Transforms](#transforms)

## Connector Config
Connector config usually differs depending on the side (sink or source). The common layout is:

```yaml
version: latest                 # any published version (e.g. 0.1.0) or latest
name: my-test-mqtt              
type: mqtt                      # along with version is used for connector's image resolution
topic: my-mqtt                  # Fluvio topic to read from (in case of sink connectors) or to write to (in case of source)
create_topic: false             # create Fluvio topic if not existed
parameters:                     # any parameters that then passed to connector's binary as cli arguments`
  param_1: "mqtt.hsl.fi"        # will be passed as `--param-1=mqtt.hsl.fi`
  param_2:                      # --param-2=foo:baz --param-2=bar
    - "foo:baz"                 
    - "bar"                     
  param_3:                      # --param-3=bar:10.0 --param-3=foo:bar --param-3=linger.ms:10
    foo: bar                    
    bar: 10.0                   
    linger.ms: 10               
  param_4: true                 # --param-4=true
  param_5: 10.0                 # --param-5=10
  param_6:                      # --param-6=-10 --param-6=-10.0
  - -10                         
  - -10.0                       
                                
secrets:                        # any parameters that then passed to connector's binary as environment variables`
  foo: bar                      
producer:                       # specifics for Fluvio Producer (usually needed for source connectors) 
  linger: 1ms                   
  batch-size: '44.0 MB'         
  compression: gzip             
consumer:                       # specifics for Fluvio Consumer (usually needed for sink connectors) 
  partition: 10                 
transforms:                     # sequence of transformations for all records passing through the connector 
  - uses: infinyon/jolt@0.1.0   # name of SmartModule in Fluvio Cluster (must be downloaded before usage by `fluvio sm download infinyon/jolt@0.1.0` command)
    with:                       # map of parameters which are passed to SmartModule. Parameters are different for each SmartModule
      spec:                     # Parameter name. Parameter value (sequence with two values in this case) will be serialized to JSON and passed as a string. 
        - operation: default
          spec:
            source: "http-connector"
```
### Transforms
The sequence of transformations that are performed on each record passed through the connector. Source connectors execute
transformations **before** sending to SPU, Sink connectors receive records from SPU **after** they have been transformed.
The main idea is to do the data transformation as close as possible to the data itself to maximize the efficiency of pipelines. 

Each transformation is defined by a SmartModule along with its parameters. In the following example:
```yaml
  - uses: infinyon/jolt@0.1.0   
    with:                       
      spec:                     
        - operation: default
          spec:
            source: "http-connector"
        - operation: remove
          spec:
            length: ""
```
we will use a SmartModule `infinyon/jolt@0.1.0` with one parameter `spec`. The `spec` itself is an array of operations but it is 
internal details of [`jolt`](../../smartmodules/jolt/README.md) SmartModule. 

Parameter values can be strings, maps, or sequences. So, in this example all values are valid:
```yaml
  - uses: mygroup/my_smartmodule@0.0.1   
    with:                       
      map_param_name:                     
        key1: value1
        key2:
            nested: "value2"
      seq_param_name: ["value1", "value2"]
      string_param_name: "value"
```

It's recommended to define all SmartModule parameters in `SmartModule.toml` file. For example, see [Jolt](../../smartmodules/jolt/SmartModule.toml)
or [Json-sql](../../smartmodules/json-sql/SmartModule.toml).

SmartModules must be downloaded to the Fluvio cluster before usage. 
It can be done by downloading from the [Hub](https://www.fluvio.io/cli/smartmodules/hub/#fluvio-hub-download):
```bash
fluvio sm download infinyon/jolt@0.1.0
```
or by loading from local wasm file using `smdk` tool:
```bash
smdk load --name "mygroup/my_smartmodule@0.0.1" --wasm-file ./local_file.wasm
```
More details can be found [here](https://www.fluvio.io/cli/smartmodules/smdk/#smdk-load).