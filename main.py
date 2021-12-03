import os
import shutil
import sqlite3
import urllib.request
import zipfile

# List of years in 'YY' format
date = ['06','07','08','09','10','11','12','13','14','15']

url = 'http://www.takakura.com/Kyoto_data/new_data201704/20{0}/20{0}.zip'

# Downloads, unzips and deletes the archive
if not os.path.exists('./Kyoto2016'):
    for i in date:
        print('download file 20{0}.zip'.format(i))
        urllib.request.urlretrieve(url.format(i), './20{0}.zip'.format(i))
        z = zipfile.ZipFile('./20{0}.zip'.format(i), 'r')
        z.extractall()
        z.close()
        os.remove('./20{0}.zip'.format(i))
else:
    print('Download skipped')
        
# Creating a database and interface for interaction
conn = sqlite3.connect("base.db")
cur = conn.cursor()
print('Database created')

# Getting a list of folders in 'YYYY' format
years = os.listdir('./Kyoto2016/')
years = list(filter(lambda x: x.isdigit(), years))
years.sort()

# Creating a table for each year
for i in years:
	cur.execute("""CREATE TABLE IF NOT EXISTS Table_{0}(id integer, date text, duration real, service text,source_bytes int, dest_bytes int, count int, Same_srv_rate real, Serror_rate real, Srv_serror real, Dst_host_count int, Dst_host_srv_count int, Dst_host_same_src_port_rate real, Dst_host_serror_rate real, Dst_hostsrv_serror_rate real, Flag text, IDS_detection text, Malware_detection text, Ashula_detection text, Label int, Source_IP_addr text, Source_port_number int, Dest_IP_addr text, Dest_port_number int, Start_time text, Protocol text);""".format(i))
	conn.commit()
print('Tables created')

# Loop through 'YYYY' folders and get list of 'MM' folders
for y in years:
	id = 0
	path_m = './Kyoto2016/' + y + '/'
	mounth = os.listdir(path_m)
	mounth.sort()
    
	# Loop through the month folder and get a list of days
	for m in mounth:
		path_d = path_m + m + '/'
		days = os.listdir(path_d)
		days.sort()
        
		# Reading lines from each file
		for day in days:
			path_f = path_d + day
			with open(path_f, "r") as file:
            
				# Adding id and date values and writing to the database
				for line in file:
					data = line.strip().split('\t')
					data.insert(0, id)
					data.insert(1, day[:8])
					id += 1
					cur.execute('''INSERT INTO Table_{0}(id, date, duration, service,source_bytes, dest_bytes, count, Same_srv_rate, Serror_rate, Srv_serror, Dst_host_count, Dst_host_srv_count, Dst_host_same_src_port_rate, Dst_host_serror_rate, Dst_hostsrv_serror_rate, Flag, IDS_detection, Malware_detection, Ashula_detection, Label, Source_IP_addr, Source_port_number, Dest_IP_addr, Dest_port_number, Start_time, Protocol) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''.format(y), data)
		
        print('Writing mounth', m, 'completed')
        
	conn.commit()
	print('Writing to a table',y,'completed')

#Удаление папки с txt
#shutil.rmtree('./Kyoto2016')
