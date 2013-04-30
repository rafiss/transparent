# Create your views here.

from django import forms
from django.contrib import auth
from django.contrib.auth.forms import UserCreationForm
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
import json, urllib2

BACKEND_URL = 'http://140.180.186.131:16317'

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
    if user is not None and user.is_active:
        # Correct password, and the user is marked "active"
        auth.login(request, user)
        # Redirect to a success page.
        return HttpResponseRedirect("/")
    else:
        # Show an error page
        return HttpResponseRedirect("/")

def logout(request):
    auth.logout(request)
    # Redirect to a success page.
    return HttpResponseRedirect("/")

def search(request):
    page = request.GET.get('p', '1')
    if 'q' in request.GET and request.GET['q']:
        query = request.GET['q']
        payload = {'select': ['name', 'image', 'price', 'model'],
                'where': {'name': '=' + query},
                'page': page,
                'limit': 500}
        resp = urllib2.urlopen(BACKEND_URL, json.dumps(payload))
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

def product(request, model):
    name = request.GET['q']
    payload = {'select': ['name', 'image', 'price', 'model'],
            'where': {'name': '=' + name[0] + '*', 'model': '=' + model},
            'limit': 1}
    resp = urllib2.urlopen(BACKEND_URL, json.dumps(payload))
    results = json.loads(resp.read())
    product = {}
    for j in range(len(payload['select'])):
        product[payload['select'][j]] = results[0][j]
    return render(request, "product.html", {'product': product})

