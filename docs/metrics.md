ModelMesh publishes a variety of metrics related to model request rates and timings, model loading/unloading rates, times and sizes, internal queuing delays, capacity/usage, cache state/LRU, and more, which can be used in addition to the Kubernetes-level resource metrics.

### Configuring metrics

By default, metrics are pushed to Sysdig via the StatsD protocol on UDP port `8126` but Prometheus-based metrics publishing (pull, instead of push) is also supported and recommended over StatsD. It is not currently the default since there are some annotations which also need to be added to the ModelMesh pod spec before Sysdig will capture the metrics (see [below](#enabling-sysdig-capture-of-prometheus-metrics)).

The `MM_METRICS` env variable can be used to configure or disable how metrics are published:

- To disable metrics, set it to `disabled`. 
- Otherwise, set to `<kind>[:param1=val1;param2=val2;...;paramN=valN]` where `<kind>` can be either `statsd` or `prometheus`, and `paramI=valI` are optional associated parameters from the table below:

|      	       |                                       Purpose                                                                          	                                        |          Applies to                    	           |              Default               |
|:------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------:|:----------------------------------:|
| `port`     	 |  Port on which to send or serve metrics                                                                                                                     	   | statsd (UDP push), prometheus (HTTP/HTTPS serve) 	 | 8126 (statsd), 2112 (prometheus) 	 |
| `fq_names` 	 |  Whether to use fully-qualified method names in request metrics                                                                                             	   | statsd, prometheus                               	 | false                            	 |
| `legacy`   	 | Whether to publish legacy flavour (non-Sysdig) statsd metrics. Note that the legacy metrics are equivalent but have different names to those in the table below | statsd                                           	 | false                            	 |
| `scheme`   	 |  Protocol scheme to use for Prometheus metrics, can be http or https                                                                                        	   | prometheus                                       	 | https                            	 |


### Capturing Prometheus metrics

Sysdig will only capture Prometheus metrics from pods with the appropriate annotations set. In addition to configuring the `MM_METRICS` env var, the following annotations must be configured on the ModelMesh deployment's Pod spec:

```     
prometheus.io/path: /metrics
prometheus.io/port: "2112"
prometheus.io/scheme: https
prometheus.io/scrape: "true"
```

### List of Exposed Metrics

|          Name                    	           | Type  	  |   Scope     	   |                Description                               	                 |
|:--------------------------------------------:|:--------:|:---------------:|:--------------------------------------------------------------------------:|
| modelmesh_invoke_model                     	 | Count  	 | (statsd only) 	 | Count of internal model server inference requests                        	 |
| modelmesh_invoke_model_milliseconds        	 | Timing 	 |        	        | Internal model server inference request time                             	 |
| modelmesh_api_request                      	 | Count  	 | (statsd only) 	 | Count of external inference requests                                     	 |
| modelmesh_api_request_milliseconds         	 | Timing 	 |        	        | External inference request time                                          	 |
| modelmesh_request_size_bytes               	 | Size   	 |        	        | Inference request payload size                                           	 |
| modelmesh_response_size_bytes              	 | Size   	 |        	        | Inference response payload size                                          	 |
| modelmesh_cache_miss                       	 | Count  	 | (statsd only) 	 | Count of inference request cache misses                                  	 |
| modelmesh_cache_miss_milliseconds          	 | Timing 	 |        	        | Cache miss delay                                                         	 |
| modelmesh_loadmodel                        	 | Count  	 | (statsd only) 	 | Count of model loads                                                     	 |
| modelmesh_loadmodel_milliseconds           	 | Timing 	 |        	        | Time taken to load model                                                 	 |
| modelmesh_loadmodel_failure                	 | Count  	 |        	        | Model load failures                                                      	 |
| modelmesh_unloadmodel                      	 | Count  	 | (statsd only) 	 | Count of model unloads                                                   	 |
| modelmesh_unloadmodel_milliseconds         	 | Timing 	 |        	        | Time taken to unload model                                               	 |
| modelmesh_unloadmodel_failure              	 | Count  	 |        	        | Unload model failures (not counting multiple attempts for same copy)     	 |
| modelmesh_unloadmodel_attempt_failure      	 | Count  	 |        	        | Unload model attempt failures                                            	 |
| modelmesh_req_queue_delay_milliseconds     	 | Timing 	 |        	        | Time spent in inference request queue                                    	 |
| modelmesh_loading_queue_delay_milliseconds 	 | Timing 	 |        	        | Time spent in model loading queue                                        	 |
| modelmesh_model_sizing_milliseconds        	 | Timing 	 |        	        | Time taken to perform model sizing                                       	 |
| modelmesh_model_evicted                    	 | Count  	 | (statsd only) 	 | Count of model copy evictions                                            	 |
| modelmesh_age_at_eviction_milliseconds     	 | Age    	 |        	        | Time since model was last used when evicted                              	 |
| modelmesh_loaded_model_size_bytes          	 | Size   	 |        	        | Reported size of loaded model                                            	 |
| modelmesh_models_loaded_total              	 | Gauge  	 | Deployment    	 | Total number of models with at least one loaded copy                     	 |
| modelmesh_models_with_failure_total        	 | Gauge  	 | Deployment    	 | Total number of models with one or more recent load failures             	 |
| modelmesh_models_managed_total             	 | Gauge  	 | Deployment    	 | Total number of models managed                                           	 |
| modelmesh_instance_lru_seconds             	 | Gauge  	 | Pod           	 | Last used time of least recently used model in pod (in secs since epoch) 	 |
| modelmesh_instance_lru_age_seconds         	 | Gauge  	 | Pod           	 | Last used age of least recently used model in pod (secs ago)             	 |
| modelmesh_instance_capacity_bytes          	 | Gauge  	 | Pod           	 | Effective model capacity of pod excluding unload buffer                  	 |
| modelmesh_instance_used_bytes              	 | Gauge  	 | Pod           	 | Amount of capacity currently in use by loaded models                     	 |
| modelmesh_instance_used_bps                	 | Gauge  	 | Pod           	 | Amount of capacity used in basis points (100ths of percent)              	 |
| modelmesh_instance_models_total            	 | Gauge  	 | Pod           	 | Number of model copies loaded in pod                                     	 |