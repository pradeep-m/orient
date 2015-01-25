import sys,traceback
sys.path.insert(0, '/Users/pradeep/protocol_buffers/protos/classes')
import pixellog_pb2
import urllib
from google.protobuf import text_format
import datetime

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return unix_time(dt) * 1000.0


def process_single_raw_pixel(line):
	fields = line.split("\t")
	querystring = fields[11]
	#print "querystring",querystring
	pixel = pixellog_pb2.PixelLog()
	pixel.ip_address = fields[4]
	pixel.user_agent = fields[10]
	date = fields[0]
	time = fields[1]
	ts = datetime.datetime.strptime(date + ' ' + time, "%Y-%m-%d %H:%M:%S")
	log_time = int(unix_time(ts))
	pixel.log_time = log_time
	for part in querystring.split("&"):
		key_value = part.split("=")
		if len(key_value) == 2:
			key = part.split("=")[0]
			value = part.split("=")[1]
			uqvalue = urllib.unquote(value)
			while uqvalue != value:
				value = uqvalue
				uqvalue = urllib.unquote(value)
			if key == "acct_id":
				pixel.acct_id= int(uqvalue)
			elif key == "can_url":
				pixel.canonical_url = uqvalue
			elif key == "is_conversion":
				pixel.is_conversion = int(uqvalue)
			elif key == "tzo":
				pixel.tzo = int(uqvalue)
			elif key == "type":
				pixel.etype = uqvalue
			elif key == "title":
				pixel.title = uqvalue.replace("\t"," ")
			elif key == "cookie2":
				pixel.uid = uqvalue
			elif key == "url":
				pixel.url = uqvalue
			elif key == "logtime":
				pixel.log_time = int(uqvalue)
			elif key == "ref":
				pixel.referrer = uqvalue
			elif key == "path":
				pixel.path = uqvalue
	return pixel

def pixel_to_tsv(pixel):
	return str(pixel.acct_id) + "\t" + pixel.uid + "\t" + str(pixel.log_time) + "\t" + pixel.etype + "\t" + pixel.url + "\t" + pixel.canonical_url + "\t" + str(pixel.is_conversion) + "\t" + str(pixel.tzo) + "\t" + pixel.title + "\t" + pixel.referrer + "\t" + pixel.path

def is_pixel_string(line):
	fields = line.split("\t")
	if (not line.startswith("#")) and len(fields) == 19 and fields[7].contains("pix.gif") and fields[13] == "Hit":
		return True
	return False


def process_file(filename,output_filename):
	output = open(output_filename,"wb")
	with open(filename,"rb") as f:
		for line in f:
			line = line.strip()
			if not line.startswith("#"): #is_pixel_string(line)
				try:
					pixel = process_single_raw_pixel(line)
					if pixel:
						#output.write(pixel.SerializeToString())
						print pixel_to_tsv(pixel)
					else:
						print "no pixel"
				except Exception:
					ex_type, ex, tb = sys.exc_info()
					traceback.print_tb(tb)
	output.close()

process_file("/Users/pradeep/Downloads/logs/E3ERHI65SM5TCG.2015-01-24-23.f2d96859","/Users/pradeep/Downloads/logs/output_log.txt")

