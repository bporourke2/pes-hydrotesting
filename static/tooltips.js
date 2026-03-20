(function () {
    var bubble = document.createElement('div');
    bubble.className = 'tt-bubble';
    document.body.appendChild(bubble);

    var currentText = null;

    document.addEventListener('mouseover', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        currentText = (el && el.dataset.tt) ? el.dataset.tt : null;
        if (!currentText) bubble.style.display = 'none';
    });

    document.addEventListener('mouseout', function (e) {
        var el = e.target.closest ? e.target.closest('[data-tt]') : null;
        if (el) {
            currentText = null;
            bubble.style.display = 'none';
        }
    });

    document.addEventListener('mousemove', function (e) {
        if (!currentText) return;

        if (bubble.style.display !== 'block') {
            bubble.textContent = currentText;
            bubble.style.display = 'block';
        }

        var x = e.clientX + 14;
        var y = e.clientY + 18;

        // Keep within viewport — check after placing
        if (x + 240 > window.innerWidth - 8)  x = e.clientX - 250;
        if (y + 80  > window.innerHeight - 8)  y = e.clientY - 90;

        bubble.style.left = x + 'px';
        bubble.style.top  = y + 'px';
    });
})();
