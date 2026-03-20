(function () {
    var bubble = document.createElement('div');
    bubble.className = 'tt-bubble';
    document.body.appendChild(bubble);

    document.addEventListener('mouseover', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (!el || !el.dataset.tt) return;

        bubble.textContent = el.dataset.tt;
        bubble.style.display = 'block';

        var r = el.getBoundingClientRect();
        var bh = bubble.offsetHeight || 60;
        var bw = bubble.offsetWidth  || 240;

        // Default: above the element, left-aligned
        var x = r.left;
        var y = r.top - bh - 8;

        // Flip below if no room above
        if (y < 8) y = r.bottom + 8;

        // Shift left if overflowing right edge
        if (x + bw > window.innerWidth - 8) x = window.innerWidth - bw - 8;
        if (x < 8) x = 8;

        bubble.style.left = x + 'px';
        bubble.style.top  = y + 'px';
    });

    document.addEventListener('mouseout', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el) bubble.style.display = 'none';
    });
})();
