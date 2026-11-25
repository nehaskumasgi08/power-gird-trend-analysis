from flask import Flask, jsonify
from flask_cors import CORS
import csv, os, time
app=Flask(__name__); CORS(app)
ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH=os.path.join(ROOT,'data','grid_load.csv')
def load_valid_rows():
    valid=[]; bad=0
    with open(CSV_PATH,newline='',encoding='utf-8') as f:
        for row in csv.DictReader(f):
            try:
                from datetime import datetime
                datetime.strptime(row['timestamp'],'%Y-%m-%d %H:%M:%S')
                zone=row['zone_id']; load=float(row['load_mw']); voltage=float(row['voltage_kv'])
                if zone not in ('ZONE_NORTH','ZONE_SOUTH','ZONE_EAST','ZONE_WEST') or not (0<load<500) or not (0<voltage<300): raise ValueError()
                valid.append((zone,load,voltage))
            except Exception: bad+=1
    return valid,bad
@app.get('/api/status')
def status():
    try:
        import pyspark
        available=True; version=pyspark.__version__
    except Exception: available=False; version=None
    return jsonify({'api':'online','spark_available':available,'spark_version':version,'data_source':'generated CSV'})
@app.post('/api/spark/benchmark')
def benchmark():
    try:
        from pyspark import SparkConf, SparkContext
    except Exception as e: return jsonify({'error':'PySpark is not installed. Run pip install -r requirements.txt. '+str(e)}),503
    rows,bad=load_valid_rows()
    conf=SparkConf().setAppName('PowerGridRDDAnalysis').setMaster('local[*]').set('spark.ui.enabled','false')
    sc=SparkContext.getOrCreate(conf); sc.setLogLevel('ERROR')
    try:
        base=sc.parallelize(rows,4).cache()
        # Execute real transformations/actions; collect only partition counts and compact benchmark results.
        partition_benchmarks={}
        for n in (2,4,8):
            rdd=sc.parallelize(rows,n)
            t=time.perf_counter(); count=rdd.map(lambda x:(x[0],x[1])).filter(lambda x:x[1]>0).count(); elapsed=time.perf_counter()-t
            per=rdd.mapPartitions(lambda it:[sum(1 for _ in it)]).collect()
            partition_benchmarks[str(n)]={'seconds':round(elapsed,6),'records_per_partition':per,'count':count}
        t=time.perf_counter(); uncached=base.map(lambda x:(x[0],x[1])).reduceByKey(lambda a,b:a+b).collect(); uncached_s=time.perf_counter()-t
        cached=base.cache(); cached.count(); t=time.perf_counter(); cached.map(lambda x:(x[0],x[1])).reduceByKey(lambda a,b:a+b).collect(); cached_s=time.perf_counter()-t
        # Include aggregateByKey and join in the executed demonstration pipeline.
        means=base.map(lambda x:(x[0],(x[1],1))).aggregateByKey((0.0,0),lambda a,b:(a[0]+b[0],a[1]+1),lambda a,b:(a[0]+b[0],a[1]+b[1]))
        capacities=sc.parallelize([('ZONE_NORTH',250),('ZONE_SOUTH',250),('ZONE_EAST',180),('ZONE_WEST',175)])
        joined=means.join(capacities).collect()
        lineage=base.map(lambda x:(x[0],x[1])).filter(lambda x:x[1]>0).reduceByKey(lambda a,b:a+b).toDebugString()
        base.unpersist()
        return jsonify({'spark_available':True,'record_count':len(rows),'bad_records':bad,'partition_benchmarks':partition_benchmarks,'uncached_seconds':round(uncached_s,6),'cached_seconds':round(cached_s,6),'joined_zone_count':len(joined),'lineage':lineage})
    finally:
        # Keep SparkContext alive for repeated dashboard calls.
        pass
if __name__=='__main__': app.run(host='127.0.0.1',port=5000,debug=True)
