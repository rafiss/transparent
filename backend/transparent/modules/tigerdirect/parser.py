import sys
import json
import html5lib
import lxml.cssselect
import lxml.etree
import traceback

PRODUCT_LIST_URL = "http://m.tigerdirect.com/applications/category/category_slc.asp?page=%d&Nav=|c:%d|lp:%s:hp:%s|&Sort=3&Recs=30"
PRODUCT_INFO_URL = "http://m.tigerdirect.com/applications/SearchTools/item-details.asp?Sku="
CATEGORIES = [	32,		22, 	99,		8, 		109,	9, 		10,		13, 	106,	24, \
				107,	5298, 	28,		3, 		98,		136, 	36,		379, 	4462,	12	]
RANGES = [ ('0.01', '24.99'), ('25.00', '49.99'), ('50.00', '99.99'), ('100.00', '199.99'), \
		('200.00', '499.99'), ('500.00', '749.99'), ('750.00', '999.99'), ('1000.00', '1499.99'), ('1500.00', '0') ]

parser = html5lib.HTMLParser(tree=html5lib.treebuilders.getTreeBuilder("lxml"), namespaceHTMLElements=False)
price_selector = lxml.cssselect.CSSSelector('dd.priceFinal')
next_selector = lxml.cssselect.CSSSelector('a.paginatNext')
name_selector = lxml.cssselect.CSSSelector('#sectionProductInfo strong')
backup_name_selector = lxml.cssselect.CSSSelector('#productDesc')
specs_selector = lxml.cssselect.CSSSelector('#DetailedSpecs')
warranty_selector = lxml.cssselect.CSSSelector('.productFootnote li')
tr_selector = lxml.cssselect.CSSSelector('tr')
th_selector = lxml.cssselect.CSSSelector('th')
td_selector = lxml.cssselect.CSSSelector('td')
product_selector = lxml.cssselect.CSSSelector('li.prodRowWrap')
id_selector = lxml.cssselect.CSSSelector('p.itemModel')

err = sys.stderr

def nodeText(node):
	return ''.join([x for x in node.itertext()])

def encodePrice(s):
	price_string = ''.join(c for c in s if c.isdigit())
	if len(price_string) == 0:
		return None
	return int(price_string)

def httpGetRequest(url):
	json.dump({'type':'get', 'url':url}, sys.stdout)
	sys.stdout.write('\n')
	sys.stdout.flush()
	return json.loads(sys.stdin.readline())['response']

def parseProductList(request):
	state = (0, 0, 1)
	if ('state' in request and len(request['state']) > 0):
		state = map(int, request['state'].split('.'))

	# for each category, parse the price ranges
	for category in range(state[0], len(CATEGORIES)):
		for price_range in range(state[1], len(RANGES)):
			page = state[2]
			while True:
				url = PRODUCT_LIST_URL % (page, CATEGORIES[category], RANGES[price_range][0], RANGES[price_range][1])
				parsed = parser.parse(httpGetRequest(url))
				products = product_selector(parsed)
				if len(products) == 0:
					break

				ids = []
				for product in products:
					ids.append(nodeText((id_selector(product))[0]).split('|')[0].replace('Item#:','').strip())
				state_string = '.'.join((str(category), str(price_range), str(page)))
				json.dump({'type':'response', 'state':state_string, 'ids':ids}, sys.stdout)
				sys.stdout.write('\n')
				sys.stdout.flush()
				if len(next_selector(parsed)) == 0:
					break
				page += 1

def parseProductInfo(request):
	response = {}
	productid = request['id']
	parsed = parser.parse(httpGetRequest(PRODUCT_INFO_URL + productid))
	selected_price = price_selector(parsed)
	if selected_price is not None and len(selected_price) > 0:
		encoded_price = encodePrice(nodeText(selected_price[0]))
		if encoded_price is not None:
			response['price'] = encoded_price
	selected_name = name_selector(parsed)
	if selected_name is None or len(selected_name) == 0:
		selected_name = backup_name_selector(parsed)
	response['name'] = nodeText(selected_name[0]).strip()

	# parse brand and model
	warranty = warranty_selector(parsed)
	if (warranty is not None and len(warranty) > 0):
		keyvalues = lxml.etree.tostring(warranty[0]).replace('<li>','').split('<br/>')
		for keyvalue in keyvalues:
			token = keyvalue.split(':')
			if len(token) > 1:
				key = token[0].strip().lower()
				value = token[1].strip().replace('<b>','').replace('</b>','') \
						.replace('<strong>','').replace('</strong>','')
				if key == 'manufactured by':
					response['brand'] = value
				elif key == 'mfg part no':
					response['model'] = value

	# parse specification key-value pairs
	specs = specs_selector(parsed)
	if specs is not None and len(specs) > 0:
		rows = tr_selector(specs[0])
		for row in rows:
			key = th_selector(row[0])
			value = td_selector(row[0])
			if value is not None and len(value) > 0:
				response[key[0].strip().lower()] = value[0]
	json.dump({'type':'response', 'response':response}, sys.stdout)
	sys.stdout.write('\n')
	sys.stdout.flush()

try:
	request = json.loads(sys.stdin.readline())
	if ('type' not in request):
		sys.exit(0)
	if (request['type'] == 'list'):
		parseProductList(request)
	elif (request['type'] == 'info'):		
		while len(request) > 0:
			parseProductInfo(request)
			request = json.loads(sys.stdin.readline())
	else:
		err.write("Unrecognized request type '" + request['type'] + "'.\n")
except:
	traceback.print_exc(file=err)
err.flush();
err.close();

