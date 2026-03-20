(function () {
    var bubble = document.createElement('div');
    bubble.className = 'tt-bubble';
    document.body.appendChild(bubble);

    var active = false;

    document.addEventListener('mouseover', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el && el.dataset.tt) {
            bubble.textContent = el.dataset.tt;
            bubble.style.display = 'block';
            active = true;
        }
    });

    document.addEventListener('mouseout', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el) {
            bubble.style.display = 'none';
            active = false;
        }
    });

    document.addEventListener('mousemove', function (e) {
        if (!active) return;
        var x = e.clientX + 14;
        var y = e.clientY + 18;

        // Flip left if bubble would overflow right edge
        bubble.style.left = x + 'px';
        bubble.style.top  = y + 'px';

        var rect = bubble.getBoundingClientRect();
        if (rect.right > window.innerWidth - 8) {
            x = e.clientX - rect.width - 10;
            bubble.style.left = x + 'px';
        }
        if (rect.bottom > window.innerHeight - 8) {
            y = e.clientY - rect.height - 10;
            bubble.style.top = y + 'px';
        }
    });
})();
