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
    margin: auto;
  }
  .grid-child {
  }
  .center {
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
<script type="text/javascript">
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
<!-- <script src="https://platform.linkedin.com/badges/js/profile.js" async defer type="text/javascript"></script> -->
![Meerschaum Banner](banner_1920x320.png)
# Welcome to Meerschaum!
Here you can find usage information, helpful guides, and tutorials.

If you'd like to incorporate Meerschaum into your project, head over to [docs.meerschaum.io](https://docs.meerschaum.io) for technical API documentation of the `meerschaum` package.

<div class="grid-container center">
  <div class="grid-child">
    <p><a id="get-started-button" class="md-button md-button--primary" href="get-started" style="float: right;">Get Started</a></p>
  </div>
  <div class="grid-child" style="float: left;">
    <p><a id="pip-button" class="md-button" href="#" style="float: left; font-family: monospace" onclick="copy_install_text(this)">$ pip install meerschaum<span class="twemoji">
</a></p>
  </div>
</div>

<div class="grid-container">
  <div class="grid-child">
    <h2>What is Meerschaum?</h2>
    <p>Meerschaum is a platform for quickly creating and managing time-series data streams called <b><a href="/reference/pipes/">pipes</a></b>. With Meerschaum, you can have a data visualization stack running in minutes.</p>
    <div style="text-align: center">
      <iframe width="480" height="270" src="https://www.youtube.com/embed/wncA_vaIois" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
    </div>
  </div>
  <div class="grid-child">
    <h2>Why Meerschaum?</h2>
    <p>If you've worked with time-series data, you know the headaches that come with ETL. Meerschaum is a system that makes consolidating and syncing data easy.</p>
    <p>Anytime you need to move time-series data from somewhere like a database, API, MQTT topic, or even CSV files (we've all done it at some point), you usually end up writing some scripts that silently break in a year.</p>
    <p>Meerschaum instead gives you better tools to define and sync your data streams. And don't worry — you can always incorporate Meerschaum into your existing scripts.</p>
  </div>
</div>

## Support Meerschaum's Development
<div class="grid-container">
  <div class="grid-child">
    <p style="text-align: left">I'm a full-time graduate student, and I work on Meerschaum in my free time. If you enjoy Meerschaum and want to support its development, feel free <a href="https://www.buymeacoffee.com/bmeares">buy me a beer (or coffee)</a>!
    </p>
    <div class="center">
      <!-- <script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="bmeares" data-color="#5F7FFF" data-emoji="🍺"  data-font="Cookie" data-text="Buy me a beer" data-outline-color="#000000" data-font-color="#ffffff" data-coffee-color="#FFDD00" ></script> -->
    </div>
  </div>
  <div class="grid-child">
    <p>If you're looking to use Meerschaum in your business and would like some help, you can commission me for my consulting services.</p><p>Additionally, in case you're hiring a remote data engineer, I'm open to offers. Reach out on <a href="https://linkedin.com/in/bennettmeares">LinkedIn</a> to get in touch.</p>
    <div style="display: flex; justify-content: center;">
      <!-- <div class="badge-base LI-profile-badge" data-locale="en_US" data-size="medium" data-theme="light" data-type="HORIZONTAL" data-vanity="bennettmeares" data-version="v1"><a class="badge-base__link LI-simple-link" href="https://www.linkedin.com/in/bennettmeares?trk=profile-badge">Bennett Meares</a></div> -->

    </div>
  </div>
</div>



