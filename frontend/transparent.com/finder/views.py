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
                'limit': '500'}
        resp = urllib2.urlopen(BACKEND_URL, json.dumps(payload))
        results = json.loads(resp.read())
        results = results[:15]
        for i in range(len(results)):
            results[i] = {'name': results[i][0], 'image': results[i][1], 'price': results[i][2], 'model': results[i][3]}
        products = []
        for i in range(0, len(results), 3):
            products.append(results[i:i+3])
        return render(request, "search.html", {'products': products})
    else:
        return HttpResponseRedirect('/')

def product(request):
    if 'q' in request.GET and request.GET['q']:
        query = request.GET['q']
        payload = {'select': ['name', 'image', 'price', 'model'],
                'where': {'name': '=' + query}}
        resp = urllib2.urlopen(BACKEND_URL, json.dumps(payload))
        results = json.loads(resp.read())
        product = {'name': results[0][0], 'image': results[0][1], 'price': results[0][2], 'model': results[0][3]}
        return render(request, "product.html", {'product': product})
    else:
        return HttpResponseRedirect(request.get_full_path())

