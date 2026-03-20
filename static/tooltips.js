(function () {
    var bubble = document.createElement('div');
    bubble.className = 'tt-bubble';
    // Start off-screen so it never flashes in a corner
    bubble.style.left = '-9999px';
    bubble.style.top  = '-9999px';
    document.body.appendChild(bubble);

    function position(clientX, clientY) {
        var x = clientX + 14;
        var y = clientY + 18;
        bubble.style.left = x + 'px';
        bubble.style.top  = y + 'px';
        // Flip if overflowing viewport edges
        var rect = bubble.getBoundingClientRect();
        if (rect.right > window.innerWidth - 8) {
            bubble.style.left = (clientX - rect.width - 10) + 'px';
        }
        if (rect.bottom > window.innerHeight - 8) {
            bubble.style.top = (clientY - rect.height - 10) + 'px';
        }
    }

    document.addEventListener('mouseover', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el && el.dataset.tt) {
            bubble.textContent = el.dataset.tt;
            bubble.style.display = 'block';
            position(e.clientX, e.clientY);
        }
    });

    document.addEventListener('mouseout', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el) {
            bubble.style.display = 'none';
            bubble.style.left = '-9999px';
            bubble.style.top  = '-9999px';
        }
    });

    document.addEventListener('mousemove', function (e) {
        if (bubble.style.display === 'block') {
            position(e.clientX, e.clientY);
        }
    });
})();
