---
hide:
  - navigation
  - toc
---
<style>
  .md-main__inner {
    margin-top: 0;
  }
  .md-sidebar {
    display: none;
  }
  .test {
    align: center;
  }
  .grid-container {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-gap: 20px;
    max-width: 100%;
    text-align: center;
    margin: auto;
  }
  .grid-child {
    text-align: center;
  }
  #pip-button {
    width: 73%;
    font-size: 1.1rem;
  }
  #get-started-button {
    font-size: 1.1rem;
    width: 73%;
  }
</style>
<script>
  function copy_install_text(btn){
    var inp =document.createElement('input');
    document.body.appendChild(inp);
    inp.value = "pip install meerschaum";
    inp.select();
    document.execCommand('copy',false);
    inp.remove();
    old_btn_text = btn.text;
    btn.text = "Copied!";
    window.setTimeout(() => {
      btn.text = old_btn_text;
    }, 2000);
   }
</script>
![Meerschaum Banner](banner_1920x320.png)
# Welcome to Meerschaum!
Here you can find usage information, helpful guides, and tutorials.

If you'd like to incorporate Meerschaum into your project, head over to [docs.meerschaum.io](https://docs.meerschaum.io) for technical API documentation of the `meerschaum` package.
<div class="grid-container">
  <div class="grid-child">
    <p><a id="get-started-button" class="md-button md-button--primary" href="get-started" style="float: right;">Get Started</a></p>
  </div>
  <div class="grid-child" style="float: left;">
    <p><a id="pip-button" class="md-button" href="#" style="float: left; font-family: monospace" onclick="copy_install_text(this)">$ pip install meerschaum<span class="twemoji">
</a></p>
  </div>
</div>

## Support Meerschaum's Development
<div class="grid-container">
  <p style="text-align: left">I'm a full-time graduate student, and I work on Meerschaum in my free time. If you enjoy Meerschaum and want to support its development, feel free <a href="https://www.buymeacoffee.com/bmeares">buy me a beer (or coffee)</a>!
  </p>
  <script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="bmeares" data-color="#5F7FFF" data-emoji="🍺"  data-font="Cookie" data-text="Buy me a beer" data-outline-color="#000000" data-font-color="#ffffff" data-coffee-color="#FFDD00" ></script>
</div>



