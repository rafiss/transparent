# Create your views here.

from django import forms
from django.contrib import auth
from django.contrib.auth.forms import UserCreationForm
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseNotFound, HttpResponseForbidden
from django.shortcuts import render
from finder.models import Module, UserProfile, Track, Product
from transparent.settings import BACKEND_URL, BACKEND_IP, MEDIA_ROOT
from datetime import datetime
import json, urllib2
import os

class UploadFileForm(forms.Form):
    module_name = forms.CharField(max_length=50)
    source_name = forms.CharField(max_length=50)
    source_url = forms.CharField(max_length=50)
    source_file = forms.FileField()

PAGE_SIZE = 15

def index(request):
    return render(request, "index.html")

def register(request):
    if request.method == 'POST':
        form = UserCreationForm(request.POST)
        if form.is_valid():
            new_user = form.save()
            return HttpResponseRedirect("/")
    else:
        form = UserCreationForm()
    return render(request, "registration/register.html", {
        'form': form,
    })

def login(request):
    username = request.POST.get('username', '')
    password = request.POST.get('password', '')
    user = auth.authenticate(username=username, password=password)
    referer = request.META.get('HTTP_REFERER', '/')
    if user is not None and user.is_active:
        # Correct password, and the user is marked "active"
        auth.login(request, user)
        # Redirect to a success page.
        return HttpResponseRedirect(referer)
    else:
        # Show an error page
        return HttpResponseRedirect(referer)

def logout(request):
    auth.logout(request)
    # Redirect to a success page.
    return HttpResponseRedirect("/")

def search(request):
    page = request.GET.get('p', '1')
    referer = request.META.get('HTTP_REFERER', '/')
    if 'q' in request.GET and request.GET['q']:
        query = request.GET['q']

        modules = []
        if request.user is not None and request.user.is_authenticated():
            modules = [module.backend_id for module in request.user.userprofile.modules.all()]

        payload = {'select': ['name', 'image', 'price', 'gid'],
                'name': query,
                'page': page,
                'pagesize': PAGE_SIZE}
        if modules:
            payload['modules'] = modules

        results = []
        resp = urllib2.urlopen(BACKEND_URL + '/search', json.dumps(payload))
        results = json.loads(resp.read())
        more = len(results) == PAGE_SIZE
        for i in range(len(results)):
            new = {}
            for j in range(len(payload['select'])):
                new[payload['select'][j]] = results[i][j]
            results[i] = new
        products = []

        for i in range(0, len(results), 3):
            products.append(results[i:i+3])
        return render(request, "search.html", {'products': products, 'query': query,
            'page': page, 'more': more})
    else:
        return HttpResponseRedirect(referer)

def product(request, gid):
    modules = Module.objects.all()
    tracking = False
    threshold = 0
    if request.user is not None and request.user.is_authenticated():
        if request.user.userprofile.modules.all():
            modules = request.user.userprofile.modules.all()
        if Product.objects.filter(gid=str(gid)):
            product = Product.objects.get(gid=str(gid))
            if Track.objects.filter(userprofile=request.user.userprofile, product=product):
                tracking = True
                track = Track.objects.get(userprofile=request.user.userprofile, product=product)
                threshold = track.threshold
                threshold = "${0:.2f}".format(round(float(threshold) / 100, 2))
    payload = {'gid': gid}
    if modules:
        payload['modules'] = [module.backend_id for module in modules]
    resp = urllib2.urlopen(BACKEND_URL + '/product', json.dumps(payload))
    product = json.loads(resp.read())

    # Remove empty module results
    included_modules = []
    for module in modules:
        if module.backend_id in product and product[module.backend_id]:
            included_modules.append(module)

    return render(request, "product.html", {'gid': gid, 'product': product,
        'modules': included_modules, 'tracking': tracking, 'threshold': threshold})

def about(request):
    return render(request, "about.html")

def how_it_works(request):
    return render(request, "how_it_works.html")

def settings(request):
    modules = Module.objects.all()
    user_modules = []
    if request.user and request.user.is_authenticated():
        user_modules = request.user.userprofile.modules.all()
    u = [um.backend_id for um in user_modules]
    return render(request, "settings.html", {'modules': modules, 'user_modules': u})

def toggle_module(request):
    if (not request.is_ajax() or
       not request.user or
       not request.user.is_authenticated() or
       not request.method == "POST"):
        return HttpResponseNotFound()

    bid = request.POST.get('bid', None)
    if bid:
        module = Module.objects.get(backend_id=bid)
        if module:
            enable = request.POST.get('enable')
            if enable == "1":
                request.user.userprofile.modules.add(module)
            else:
                request.user.userprofile.modules.remove(module)
            request.user.userprofile.save()
    return HttpResponse()

def moduleAPI(request):
    return render(request, "moduleAPI.html", {})

def tracked_items(request):
    tracks = []
    if request.user and request.user.is_authenticated():
        tracks = request.user.userprofile.track_set.all()
    products = [{'gid': track.product.gid,
        'price': "{0:.2f}".format(round(float(track.product.price) / 100, 2)),
        'threshold': "{0:.2f}".format(round(float(track.threshold) / 100, 2)),
        'name': track.product.name}
        for track in tracks]
    return render(request, "tracked_items.html", {'products': products})

def track(request):
    if not (request.user and request.user.is_authenticated() and request.method == 'POST'):
        return HttpResponseRedirect('/404')
    gid = request.POST.get('gid', None)
    price = request.POST.get('price', None)
    threshold = request.POST.get('threshold', None)
    name = request.POST.get('name', None)

    parsed_threshold = threshold
    gid = str(gid)
    if price and price[0] == '$':
        price = price[1:]
    try:
        price = int(100 * float(price))
    except ValueError:
        return HttpResponseRedirect('/404')
    if threshold and threshold[0] == '$':
        parsed_threshold = threshold[1:]
    try:
        parsed_threshold = int(100 * float(parsed_threshold))
    except ValueError:
        return HttpResponseRedirect('/404')

    if not (gid and price and parsed_threshold and price > 0 and parsed_threshold > 0):
        return HttpResponseRedirect('/404')
    product, created = Product.objects.get_or_create(gid=gid,
            defaults={'price': price, 'name': name})
    product.save()
    track = Track(userprofile=request.user.userprofile, product=product, threshold=parsed_threshold)
    track.save()

    resp = urllib2.urlopen(BACKEND_URL + '/subscribe',
            json.dumps({'gid': int(gid), 'price': threshold}))
    json.loads(resp.read())

    return HttpResponseRedirect('/product/{0}'.format(gid))

def stop_track(request):
    if not (request.user and request.user.is_authenticated() and request.method == 'POST'):
        return HttpResponseRedirect('/404')
    gid = request.POST.get('gid', None)
    threshold = request.POST.get('gid', None)

    resp = urllib2.urlopen(BACKEND_URL + '/unsubscribe', 
            json.dumps({'gid': int(gid), 'price': threshold}))
    json.loads(resp.read())
 
    product = Product.objects.get(gid=str(gid))
    track = Track.objects.get(userprofile=request.user.userprofile, product=product)
    track.delete()
   
    return HttpResponseRedirect('/product/{0}'.format(gid))

def track_notify(request):
    if request.META['REMOTE_ADDR'] != BACKEND_IP:
        return HttpResponseNotFound()
    payload = json.loads(request.body)
    product = Product.objects.get(gid=str(payload['gid']))
    price = payload['price']
    price = int(100 * float(price[1:]))
    product = Product(price=price)
    product.save()

def submit(request):
    newForm = UploadFileForm()
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            module_name = request.POST.get('module_name', None)
            source_name = request.POST.get('source_name', None)
            source_url = request.POST.get('source_url', None)
            source_code = request.FILES['source_file']
 
            now = str(datetime.now())
            directory = os.path.join(MEDIA_ROOT, 'modules', now)
            if not os.path.exists(directory):
                os.makedirs(directory)
            info_file = open(os.path.join(directory, 'info.txt'), 'w+')
            source_file = open(os.path.join(directory, module_name), 'w+')
            info_file.write('Module Name: ' + module_name + '\n')
            info_file.write('Source Name: ' + source_name + '\n')
            info_file.write('Source URL: ' + source_url + '\n')
            for chunk in source_code.chunks():
                source_file.write(chunk)
            info_file.close()
            source_file.close()
            return render(request, "submit.html", {'succeeded': True, 'form': newForm})
        else:
            return render(request, "submit.html", {'failed': True, 'form': newForm}) 
    
    return render(request, 'submit.html', {'form': newForm})

# redirect module link to the source code on github
#def module(request)
#	return HttpResponse('https://github.com/rafiss/transparent/tree/master/backend/transparent/modules/amazon')
