import scidb
sdb = scidb.iquery()

arrayNames = sdb.list('arrays')

for name in arrayNames:
	if 'multi' in name: 
		sdb.query("remove(%s)" % (name))
		print("Removed array: %s" % (name))


