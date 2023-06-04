---
hide:
  - navigation
  - toc
---
<style>
  .md-main__inner {
    margin-top: 0;
  }
@media screen and (min-width: 76.1875em) {
  .md-sidebar {
    display: none;
  }
  #pip-button {
    width: 20em;
    font-size: 1.1rem;
    float: left;
    cursor: pointer;
  }
  #get-started-button {
    font-size: 1.1rem;
    width: 20em;
    cursor: pointer;
  }
}
@media screen and (max-width: 76.1875em) {
  #pip-button {
    font-size: 1.0rem;
    width: 20em;
    cursor: pointer;
  }
  #get-started-button {
    font-size: 1.0rem;
    width: 20em;
    cursor: pointer;
    margin-bottom: 20px;
  }
}
  .test {
    align: center;
  }

.center {
    text-align: center;
  }

h1 {
  display: none;
}

</style>
<script type="text/javascript">
  function copy_install_text(btn){
    var inp = document.createElement('input');
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
    return false;
   }
</script>

<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<link rel="stylesheet" type="text/css" href="/assets/css/grid.css" />
<script src="/assets/js/asciinema-player.js"></script>

<!-- <script src="https://platform.linkedin.com/badges/js/profile.js" async defer type="text/javascript"></script> -->
![Meerschaum Banner](banner_1920x320.png)

<!-- # Welcome to the Meerschaum Documentation Home Page -->

<!-- If you'd like to incorporate Meerschaum into your project, head over to [docs.meerschaum.io](https://docs.meerschaum.io) for technical API documentation of the `meerschaum` package. -->

<p style="text-align:center; color:#666666; font-size: 1.2em"><i>Out-of-the-box ETL, easy to learn, and a pleasure to use!</i></p>

<div class="grid-container center">
  <div class="grid-child">
    <a id="get-started-button" class="md-button md-button--primary" href="get-started">Get Started</a>
  </div>
  <div class="grid-child" >
    <a id="pip-button" class="md-button" href="#!" style="font-family: monospace" onclick="copy_install_text(this)">$ pip install meerschaum<span class="twemoji">
</a>
  </div>
</div>

<div class="grid-container">
  <div class="grid-child">
    <h2>What is Meerschaum?</h2>
    <p>Meerschaum is a platform for quickly creating and managing time-series data streams called <b><a href="/reference/pipes/">pipes</a></b>. With Meerschaum, you can have a data visualization stack running in minutes.</p>
    <h2>Why Meerschaum?</h2>
    <p>Two words: <i>Incremental updates</i>. Fetch the data you need, and Meerschaum will handle the rest.</p>
    <p>If you've worked with time-series data, you know the headaches that come with ETL. Meerschaum is a system that makes consolidating and syncing data easy.</p>
    <p>Meerschaum instead gives you better tools to define and sync your data streams. And don't worry — you can always incorporate Meerschaum into your existing systems.</p>

  </div>
  <div class="grid-child">
    <br>
    <asciinema-player src="/assets/casts/demo.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>
  </div>
</div>

<h2>✨ Features</h2>
<div class="grid-container">
  <div class="grid-child">

  <h3>Organize ETL Processes into <a href="/reference/pipes">Pipes</a></h3>

  <p>Meerschaum <a href="/reference/pipes">Pipes</a> are parametrized ETL processes that are tagged and organized into hierarchies to make scaling up a breeze.</p>

  <img src="/assets/screenshots/show-pipes-fred.png"></img>
  <img src="/assets/screenshots/show-pipes-chicken.png"></img>

  <h3>Robust <a href="/reference/plugins/writing-plugins/">Plugin System</a></h3>
  <p>Plugins make it easy to ingest any data source, add functionality to Meerschaum, and organize your utility scripts.</p>
  <img src="/assets/screenshots/plugin-init.png"></img>


  <h3>Clean Connector Management</h3>

  <p>Define your connectors at any level: through the CLI, in your environment, or dynamically.</p>

  ```bash
  ### You can follow an interactive wizard.
  mrsm bootstrap connector

  ### Or define connectors in your environment.
  export MRSM_SQL_BAZ='postgresql://foo:bar@localhost:5432/baz'
  export MRSM_MONGODB_LOCAL='{
    "uri": "mongodb://localhost:27017",
    "database": "meerschaum"
  }'
  ```

  ```python
  ### Or you can build connectors on-the-fly in code.
  import meerschaum as mrsm
  conn = mrsm.get_connector('sql:demo', flavor='sqlite', database='/tmp/demo.db')
  ```

  <h3>Design, Develop, and Deploy with <a href="/reference/compose">Meerschaum Compose</a></h3>

  <p>The <a href="/reference/compose">compose</a> workflow allows you to iterate with and version-control your pipes, making collaboration and maintainability much smoother.</p>
  
  <img src="/assets/screenshots/mrsm-compose-techslamneggs.png"></img>

  </div>
  <div class="grid-child">

    <h3>Performant SQL Transformations</h3>

    <p>SQL pipes with the same instance and source connector are synced in-place ― deltas are resolved entirely through SQL and nothing is loaded into RAM.</p>

    <img src="/assets/screenshots/sql-inplace.png"></img>

    <h4>Concurrency at its Finest</h4>

    <p>Maximize your throughput by syncing multiple pipes in parallel.</p>

    <img src="/assets/screenshots/sql-inplace-parallel.png"></img>

    <h4>Elegant Chunking</h4>

    <p>Calm your out-of-memory fears with automatic, parallelized chunking.</p>

    <img src="/assets/screenshots/sql-chunks.png"></img>

    <h3>Simple-Yet-Powerful API</h3>

    Want to use Meerschaum in your code? Check out the <a href="https://docs.meerschaum.io">package documentation</a>!

    ```python
    import meerschaum as mrsm
    conn = mrsm.get_connector("sql:demo", uri="sqlite:////tmp/demo.db")

    pipe = mrsm.Pipe(
        'foo', 'bar',
        instance = conn,
        columns = {'datetime': 'dt', 'id': 'id'},
        dtypes = {'attrs': 'json'},
    )
    docs = [
        {'dt': '2023-01-01', 'id': 1, 'val': 123.4},
        {'dt': '2023-01-01', 'id': 2, 'val': 567.8},
    ]
    pipe.sync(docs)

    docs = [
        {'dt': '2023-01-01', 'id': 1, 'attrs': {'foo': 'bar'}},
    ]
    pipe.sync(docs)

    df = pipe.get_data(params={'id': [1, 2]})
    print(df)
    #           dt  id    val           attrs
    # 0 2023-01-01   1  123.4  {'foo': 'bar'}
    # 1 2023-01-01   2  567.8            None
    ```

  <h3>Extensible Connectors Interface</h3>

  Add custom connector types with the <a href="/reference/plugins/writing-plugins/#the-make_connector-decorator"><code class="highlight"><span class="nd">@make_connector</span></code> decorator</a>.

  ```python
  from meerschaum.connectors import make_connector, Connector
  required = ['requests']

  @make_connector
  class NWSConnector(Connector):

      REQUIRED_ATTRIBUTES = ['username', 'password']

      def fetch(self, pipe, begin=None, end=None, **kw):
          params = {}
          begin = begin or pipe.get_sync_time()
          if begin:
              params['start'] = begin.isoformat()
          if end:
              params['end'] = end.isoformat()

          stations = pipe.parameters.get('nws', {}).get('stations', [])
          for station in stations:
              url = f"https://api.weather.gov/stations/{station}/observations"
              response = self.session.get(url, params=params)
              yield [
                feature['properties']
                for feature in response.json()['features']
              ]

      @property
      def session(self):
          _sesh = self.__dict__.get('_session', None)
          if _sesh is not None:
              return _sesh
          import requests
          self._session = requests.Session()
          self._session.auth = (self.username, self.password)
          return self._session
  ```

  </div>
</div>

<h2>Video Tutorials</h2>

<div class="grid-container">
  <div class="grid-child">

  <div style="text-align: center">
    <iframe width="672" height="378" src="https://www.youtube.com/embed/t9tFD4afSD4" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
  </div>


  </div>
  <div class="grid-child">

  <div style="text-align: center">
    <iframe width="672" height="378" src="https://www.youtube.com/embed/iOhPn4RjImQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
  </div>

  </div>
</div>

<div class="grid-container">
  <div class="grid-child">
  <h2>Support the Project</h2>
    <p style="text-align: left">I work on Meerschaum in my free time, so if you enjoy the project and want to support its development, feel free to <a href="https://www.buymeacoffee.com/bmeares">buy me a coffee</a>! You can also support the project on my <a href="https://github.com/sponsors/bmeares">GitHub Sponsors page</a>.
    </p>
    <div class="center">
      <!-- <script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="bmeares" data-color="#5F7FFF" data-emoji="🍺"  data-font="Cookie" data-text="Buy me a beer" data-outline-color="#000000" data-font-color="#ffffff" data-coffee-color="#FFDD00" ></script> -->
    </div>
  </div>
  <div class="grid-child">
    <h2>Consulting Services</h2>
    <p>If you're looking to recruit my skills, you can hire my consulting services. Reach out on <a href="https://linkedin.com/in/bennettmeares">LinkedIn</a> to get in touch, or you can commission my help at my <a href="https://github.com/sponsors/bmeares">GitHub Sponsors page</a>.</p>
  </div>
</div>
