'use strict';

const e = React.createElement;

class ActionButton extends React.Component {
  constructor(props) {
    super(props);
    this.state = { liked: false };
  }

  render() {
    if (this.state.liked) {
      return 'You liked this.';
    }

    return e(
      'select'
    );

    // return e(
      // 'button',
      // { onClick: () => this.setState({ liked: true }) },
      // 'Like'
    // );
  }
}

const domContainer = document.querySelector('#action_button_container');
ReactDOM.render(e(ActionButton), domContainer);
