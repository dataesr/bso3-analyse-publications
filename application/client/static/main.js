$('#all').on('click', function() {
    $.ajax({
        url: '/tasks/<task_id>',
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        method: 'GET'
    })
    .done(res => {
        $('#res_inpi').html(res.status);
    })
    .fail(err => {
        console.log(err)
    });
});