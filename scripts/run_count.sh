#java -Xmx700m -cp cqels-1.1.1-SNAPSHOT.jar test queries/q1.sparql colmap/rdb2rdf.csv ../knoesis_observations_ike_csv/ AIRGL 1000 > output/q1.txt
for ((i=$1;i<=$2;i++))
do
	java -Xmx700m -cp cqels-1.1.1-SNAPSHOT.jar test queries/q$i.sparql colmap/alpha.csv ../knoesis_observations_ike_csv/ ALPHA $3 101 colmap/ALPHA_meta.nt true > output/q$i.txt
done
