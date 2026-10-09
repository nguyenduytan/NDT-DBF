(() => {
  'use strict';
  const status = document.createElement('div');
  status.className = 'copy-status';
  status.setAttribute('role', 'status');
  status.hidden = true;
  document.body.append(status);
  let statusTimer;
  document.querySelectorAll('pre > code').forEach(code => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'copy-button';
    button.textContent = 'Copy';
    button.setAttribute('aria-label', 'Copy code');
    code.parentElement.prepend(button);
    button.addEventListener('click', async () => {
      button.disabled = true;
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(code.textContent);
        } else {
          const field = document.createElement('textarea');
          field.value = code.textContent;
          field.style.position = 'fixed';
          field.style.left = '-9999px';
          document.body.append(field);
          field.select();
          try {
            if (!document.execCommand('copy')) throw new Error('Clipboard unavailable');
          } finally {
            field.remove();
            button.focus();
          }
        }
        button.textContent = 'Copied';
        status.textContent = 'Code copied to clipboard.';
      } catch {
        const range = document.createRange();
        range.selectNodeContents(code);
        const selection = window.getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
        status.textContent = 'Copy failed. Code is selected; copy it from your browser.';
      } finally {
        button.disabled = false;
        status.hidden = false;
        clearTimeout(statusTimer);
        statusTimer = setTimeout(() => { status.hidden = true; }, 4000);
        setTimeout(() => { button.textContent = 'Copy'; }, 2000);
      }
    });
  });

  const sidebar = document.querySelector('.docs-sidebar');
  if (!sidebar) return;
  const sections = Array.from(document.querySelectorAll('.doc-section[id]'));
  const links = Array.from(sidebar.querySelectorAll('nav a[href^="#"]'));
  const input = document.getElementById('docs-search');
  const results = document.getElementById('search-results');
  const index = sections.map(section => ({
    id: section.id,
    title: section.querySelector('h2').textContent,
    text: section.textContent.toLowerCase()
  }));
  input.addEventListener('input', () => {
    const terms = input.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
    results.replaceChildren();
    results.hidden = terms.length === 0;
    if (!terms.length) return;
    const matches = index.filter(entry => terms.every(term => entry.text.includes(term)));
    const count = document.createElement('p');
    count.className = matches.length ? 'search-count' : 'search-empty';
    count.textContent = matches.length ? matches.length + ' matching topics' : 'No matching topics. Try another search.';
    results.append(count);
    matches.forEach(entry => {
      const link = document.createElement('a');
      link.href = '#' + entry.id;
      link.textContent = entry.title;
      results.append(link);
    });
  });
  const contents = sidebar.querySelector('.mobile-nav');
  const mobile = window.matchMedia('(max-width: 720px)');
  const resize = () => { contents.open = !mobile.matches; };
  resize();
  mobile.addEventListener('change', resize);
  sidebar.addEventListener('click', event => {
    if (event.target.closest('a[href^="#"]') && mobile.matches) contents.open = false;
  });
  const markCurrent = id => {
    links.forEach(link => {
      if (link.hash === '#' + id) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    });
  };
  let pending = false;
  const update = () => {
    pending = false;
    const offset = document.querySelector('.site-header').offsetHeight + 32;
    let current = sections[0];
    for (const section of sections) {
      if (section.getBoundingClientRect().top <= offset) current = section;
    }
    if (current) markCurrent(current.id);
  };
  window.addEventListener('scroll', () => {
    if (!pending) {
      pending = true;
      requestAnimationFrame(update);
    }
  }, { passive: true });
  window.addEventListener('hashchange', () => markCurrent(location.hash.slice(1)));
  if (location.hash) markCurrent(location.hash.slice(1));
  else update();
})();
