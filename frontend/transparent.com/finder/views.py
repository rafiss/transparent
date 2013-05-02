# Create your views here.

from django import forms
from django.contrib import auth
from django.contrib.auth.forms import UserCreationForm
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from finder.models import Module, UserProfile
import json, urllib2

BACKEND_URL = 'http://140.180.186.131:16317'
PAGE_SIZE = 15

def hello(request):
    return HttpResponse("Hello world")

def index(request):
    return render(request, "index.html", {})

def profile(request):
    modules = request.user.userprofile.modules
    return render(request, "profile.html", {'modules' : modules})

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
    if 'q' in request.GET and request.GET['q']:
        query = request.GET['q']

        modules = [module.backend_id for module in Module.objects.all()]
        if request.user is not None and request.user.is_active:
            modules = [module.backend_id for module in user.userprofile.modules]

        payload = {'select': ['name', 'image', 'price', 'gid'],
                'where': {'name': '=' + query},
                'modules': modules
                'page': page,
                'page_size': PAGE_SIZE,
                'limit': 500}
        resp = urllib2.urlopen(BACKEND_URL + '/search/', json.dumps(payload))
        results = json.loads(resp.read())
        results = results[:15]
        for i in range(len(results)):
            new = {}
            for j in range(len(payload['select'])):
                new[payload['select'][j]] = results[i][j]
            results[i] = new
        products = []
        for i in range(0, len(results), 3):
            products.append(results[i:i+3])
        return render(request, "search.html", {'products': products, 'query': query})
    else:
        return HttpResponseRedirect('/')

def product(request, gid):
    #payload = {'select': ['name', 'image', 'price', 'model'],
            #'where': {'name': '=' + name[0] + '*', 'model': '=' + model},
            #'limit': 1}
    modules = [module.backend_id for module in Module.objects.all()]
    if request.user is not None and request.user.is_active:
        modules = [module.backend_id for module in user.userprofile.modules]
    payload = {'gid': gid, 'modules': modules}
    resp = urllib2.urlopen(BACKEND_URL + '/product/', json.dumps(payload))
    results = json.loads(resp.read())
    product = {}
    for j in range(len(payload['select'])):
        product[payload['select'][j]] = results[0][j]
    return render(request, "product.html", {'product': product})

def about(request):
    return render(request, "about.html", {})

def how_it_works(request):
    return render(request, "how_it_works.html", {})

def modules(request):
    return render(request, "modules.html", {})

def settings(request):
    return render(request, "settings.html", {})

def selected_modules(request):
    return render(request, "selected_modules.html", {})

def tracked_items(request):
    return render(request, "tracked_items.html", {})

def submit(request):
    if request.method == 'GET':
        return render(request, "submit.html", {})
    else:
        return render(request, "submit.html", {})

