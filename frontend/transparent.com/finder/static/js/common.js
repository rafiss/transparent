function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie != '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) == (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

var csrftoken = getCookie('csrftoken');

function csrfSafeMethod(method) {
    // these HTTP methods do not require CSRF protection
    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
}

$.ajaxSetup({
    crossDomain: false,
    beforeSend: function(xhr, settings) {
        if (!csrfSafeMethod(settings.type)) {
            xhr.setRequestHeader("X-CSRFToken", csrftoken);
        }
    }
});

function toggleModule(checkBox) {
    $.ajax({
        type: "POST",
        url: "/toggle/",
        data: {"bid":checkBox.value, "enable": (checkBox.checked) ? 1 : 0}
    });
}

function upvote(btn) {
     $.ajax({
        type: "POST",
        url: "/upvote/",
        data: {"bid":btn.value},
        success: function(data, stat, jqXHR) {
            location.reload();
        }
    });
}

function downvote(btn) {
     $.ajax({
        type: "POST",
        url: "/downvote/",
        data: {"bid":btn.value},
        success: function(data, stat, jqXHR) {
            location.reload();
        }
    });
}
