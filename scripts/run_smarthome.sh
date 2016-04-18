for ((i=$1;i<=$2;i++))
do
	java -Xmx700m -cp cqels-1.1.1-SNAPSHOT.jar testSmartHome queries/smarthome/q$i.sparql ../smarthome_data/all-environmental-sort.csv ../smarthome_data/all-meter-replace.csv ../smarthome_data/all-motion-replace.csv $3 101 false > output_smarthome/q$i.txt
done
