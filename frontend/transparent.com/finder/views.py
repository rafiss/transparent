# Create your views here.

from django import forms
from django.contrib import auth
from django.contrib.auth.forms import UserCreationForm
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseNotFound, HttpResponseForbidden
from django.shortcuts import render
from finder.models import Module, UserProfile, Track, Product
from transparent.settings import BACKEND_URL, BACKEND_IP
import json, urllib2

PAGE_SIZE = 15

def index(request):
    return render(request, "index.html", {})

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
    if request.user is not None and request.user.is_authenticated():
        modules = request.user.userprofile.modules.all()
    payload = {'gid': gid}
    if modules:
        payload['modules'] = [module.backend_id for module in modules]
    resp = urllib2.urlopen(BACKEND_URL + '/product', json.dumps(payload))
    product = json.loads(resp.read())
    return render(request, "product.html", {'gid': gid, 'product': product, 'modules': modules})

def about(request):
    return render(request, "about.html", {})

def how_it_works(request):
    return render(request, "how_it_works.html", {})

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
        tracks = Track.objects.filter(userprofile=request.user.userprofile)
    products = [{'gid': track.product.gid,
        'price': "{0:.2f}".format(round(float(track.product.price) / 100, 2)),
        'threshold': "{0:.2f}".format(round(float(track.threshold) / 100, 2)),
        'name': track.product.name}
        for track in tracks]
    return render(request, "tracked_items.html", {'products': products})

def track(request):
    if not (request.user and request.user.is_authenticated() and request.method == 'POST'):
        return HttpResponseForbidden("error 403")
    gid = request.POST.get('gid', None)
    price = request.POST.get('price', None)
    threshold = request.POST.get('threshold', None)
    name = request.POST.get('name', None)
    if not (gid and price and threshold):
        return HttpResponseNotFound("{0} {1} {2}".format(gid, price, threshold))
    product, created = Product.objects.get_or_create(gid=int(gid),
            defaults={'price': int(100 * float(price[1:])),
                'name': name})
    product.save()
    track = Track(userprofile=request.user.userprofile, product=product, threshold=int(100*float(threshold)))
    track.save()
    return HttpResponseRedirect('/product/{0}'.format(gid))

def track_notify(request):
    if request.META['REMOTE_ADDR'] != BACKEND_IP:
        return HttpResponseNotFound()
    payload = json.loads(request.body)
    # TODO: Notify user. For now, user must go to tracked_items page.

def submit(request):
    if request.method == 'GET':
        return render(request, "submit.html", {})
    else:
        return render(request, "submit.html", {})

# redirect module link to the source code on github
#def module(request)
#	return HttpResponse('https://github.com/rafiss/transparent/tree/master/backend/transparent/modules/amazon')
