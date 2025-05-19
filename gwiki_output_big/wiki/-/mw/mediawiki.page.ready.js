mw.loader.impl(function(){return["mediawiki.page.ready@",{"main":"ready.js","files":{"ready.js":function(require,module,exports){var checkboxShift = require( './checkboxShift.js' );
var config = require( './config.json' );
var teleportTarget = require( './teleportTarget.js' );

// Break out of framesets
if ( mw.config.get( 'wgBreakFrames' ) ) {
	// Note: In IE < 9 strict comparison to window is non-standard (the standard didn't exist yet)
	// it works only comparing to window.self or window.window (https://stackoverflow.com/q/4850978/319266)
	if ( false ) {
		// Un-trap us from framesets
		window.top.location.href = location.href;
	}
}

mw.hook( 'wikipage.content' ).add( function ( $content ) {
	var modules = [];

	var $collapsible;
	if ( config.collapsible ) {
		$collapsible = $content.find( '.mw-collapsible' );
		if ( $collapsible.length ) {
			modules.push( 'jquery.makeCollapsible' );
		}
	}

	var $sortable;
	if ( config.sortable ) {
		$sortable = $content.find( 'table.sortable' );
		if ( $sortable.length ) {
			modules.push( 'jquery.tablesorter' );
		}
	}

	if ( modules.length ) {
		// Both modules are preloaded by Skin::getDefaultModules()
		mw.loader.using( modules ).then( function () {
			// For tables that are both sortable and collapsible,
			// it must be made sortable first and collapsible second.
			// This is because jquery.tablesorter stumbles on the
			// elements inserted by jquery.makeCollapsible (T64878)
			if ( $sortable && $sortable.length ) {
				$sortable.tablesorter();
			}
			if ( $collapsible && $collapsible.length ) {
				$collapsible.makeCollapsible();
			}
		} );
	}
	if ( $content[ 0 ] && $content[ 0 ].isConnected === false ) {
		mw.log.warn( 'wikipage.content hook should not be fired on unattached content' );
	}

	checkboxShift( $content.find( 'input[type="checkbox"]:not(.noshiftselect)' ) );
} );

// Handle elements outside the wikipage content
$( function () {
	/**
	 * There is a bug on iPad and maybe other browsers where if initial-scale is not set
	 * the page cannot be zoomed. If the initial-scale is set on the server side, this will result
	 * in an unwanted zoom on mobile devices. To avoid this we check innerWidth and set the initial-scale
	 * on the client where needed. The width must be synced with the value in Skin::initPage.
	 * More information on this bug in [[phab:T311795]].
	 *
	 * @ignore
	 */
	function fixViewportForTabletDevices() {
		var $viewport = $( 'meta[name=viewport]' );
		var content = $viewport.attr( 'content' );
		var scale = window.outerWidth / window.innerWidth;
		// This adjustment is limited to tablet devices. It must be a non-zero value to work.
		// (these values correspond to @width-breakpoint-tablet and @width-breakpoint-desktop
		if ( window.innerWidth >= 720 && window.innerWidth <= 1000 &&
			content && content.indexOf( 'initial-scale' ) === -1
		) {
			// Note: If the value is 1 the font-size adjust feature will not work on iPad
			$viewport.attr( 'content', 'width=1000,initial-scale=' + scale );
		}
	}

	// Add accesskey hints to the tooltips
	$( '[accesskey]' ).updateTooltipAccessKeys();

	var node = document.querySelector( '.mw-indicators' );
	if ( node && node.children.length ) {
		/**
		 * Fired when indicators are being added to the DOM
		 *
		 * @event ~'wikipage.indicators'
		 * @memberof Hooks
		 * @param {jQuery} $content jQuery object with the elements of the indicators
		 */
		mw.hook( 'wikipage.indicators' ).fire( $( node.children ) );
	}

	var $content = $( '#mw-content-text' );
	// Avoid unusable events, and the errors they cause, for custom skins that
	// do not display any content (T259577).
	if ( $content.length ) {
		/**
		 * Fired when wiki content has been added to the DOM.
		 *
		 * This should only be fired after $content has been attached.
		 *
		 * This includes the ready event on a page load (including post-edit loads)
		 * and when content has been previewed with LivePreview.
		 *
		 * @event ~'wikipage.content'
		 * @memberof Hooks
		 * @param {jQuery} $content The most appropriate element containing the content,
		 *   such as #mw-content-text (regular content root) or #wikiPreview (live preview
		 *   root)
		 */
		mw.hook( 'wikipage.content' ).fire( $content );
	}

	var $nodes = $( '.catlinks[data-mw="interface"]' );
	if ( $nodes.length ) {
		/**
		 * Fired when categories are being added to the DOM
		 *
		 * It is encouraged to fire it before the main DOM is changed (when $content
		 * is still detached).  However, this order is not defined either way, so you
		 * should only rely on $content itself.
		 *
		 * This includes the ready event on a page load (including post-edit loads)
		 * and when content has been previewed with LivePreview.
		 *
		 * @event ~'wikipage.categories'
		 * @memberof Hooks
		 * @param {jQuery} $content The most appropriate element containing the content,
		 *   such as .catlinks
		 */
		mw.hook( 'wikipage.categories' ).fire( $nodes );
	}

	$nodes = $( 'table.diff[data-mw="interface"]' );
	if ( $nodes.length ) {
		/**
		 * Fired when the diff is added to a page containing a diff
		 *
		 * Similar to the {@link Hooks~'wikipage.content' wikipage.content hook}
		 * $diff may still be detached when the hook is fired.
		 *
		 * @event ~'wikipage.diff'
		 * @memberof Hooks
		 * @param {jQuery} $diff The root element of the MediaWiki diff (`table.diff`).
		 */
		mw.hook( 'wikipage.diff' ).fire( $nodes.eq( 0 ) );
	}

	$( '#t-print a' ).on( 'click', function ( e ) {
		window.print();
		e.preventDefault();
	} );

	var $permanentLink = $( '#t-permalink a' );
	function updatePermanentLinkHash() {
		if ( mw.util.getTargetFromFragment() ) {
			$permanentLink[ 0 ].hash = location.hash;
		} else {
			$permanentLink[ 0 ].hash = '';
		}
	}
	if ( $permanentLink.length ) {
		$( window ).on( 'hashchange', updatePermanentLinkHash );
		updatePermanentLinkHash();
	}

	/**
	 * Fired when a trusted UI element to perform a logout has been activated.
	 *
	 * This will end the user session, and either redirect to the given URL
	 * on success, or queue an error message via mw.notification.
	 *
	 * @event ~'skin.logout'
	 * @memberof Hooks
	 * @param {string} href Full URL
	 */
	var LOGOUT_EVENT = 'skin.logout';
	function logoutViaPost( href ) {
		mw.notify(
			mw.message( 'logging-out-notify' ),
			{ tag: 'logout', autoHide: false }
		);
		var api = new mw.Api();
		api.postWithToken( 'csrf', {
			action: 'logout'
		} ).then(
			function () {
				location.href = href;
			},
			function ( err, data ) {
				mw.notify(
					api.getErrorMessage( data ),
					{ type: 'error', tag: 'logout', autoHide: false }
				);
			}
		);
	}
	// Turn logout to a POST action
	mw.hook( LOGOUT_EVENT ).add( logoutViaPost );
	$( config.selectorLogoutLink ).on( 'click', function ( e ) {
		mw.hook( LOGOUT_EVENT ).fire( this.href );
		e.preventDefault();
	} );
	fixViewportForTabletDevices();

	teleportTarget.attach();
} );

/**
 * @private
 * @param {HTMLElement} element
 * @return {boolean} Whether the element is a search input.
 */
function isSearchInput( element ) {
	return element.id === 'searchInput' ||
		element.classList.contains( 'mw-searchInput' );
}

/**
 * Load a given module when a search input is focused.
 *
 * @memberof mediawiki.page.module:ready
 * @param {string} moduleName Name of a module
 */
function loadSearchModule( moduleName ) {
	// T251544: Collect search performance metrics to compare Vue search with
	// mediawiki.searchSuggest performance. Marks and Measures will only be
	// recorded on the Vector skin.
	//
	// Vue search isn't loaded through this function so we are only collecting
	// legacy search performance metrics here.

	var shouldTestSearch = !!( moduleName === 'mediawiki.searchSuggest' &&
		mw.config.get( 'skin' ) === 'vector' &&
		window.performance &&
		performance.mark &&
		performance.measure &&
		performance.getEntriesByName ),
		loadStartMark = 'mwVectorLegacySearchLoadStart',
		loadEndMark = 'mwVectorLegacySearchLoadEnd';

	function requestSearchModule() {
		if ( shouldTestSearch ) {
			performance.mark( loadStartMark );
		}
		mw.loader.using( moduleName, function () {
			if ( shouldTestSearch && performance.getEntriesByName( loadStartMark ).length ) {
				performance.mark( loadEndMark );
				performance.measure( 'mwVectorLegacySearchLoadStartToLoadEnd', loadStartMark, loadEndMark );
			}
		} );
	}

	// Load the module once a search input is focussed.
	function eventListener( e ) {
		if ( isSearchInput( e.target ) ) {
			requestSearchModule();

			document.removeEventListener( 'focusin', eventListener );
		}
	}

	// Load the module now if the search input is already focused,
	// because the user started typing before the JavaScript arrived.
	if ( document.activeElement && isSearchInput( document.activeElement ) ) {
		requestSearchModule();
		return;
	}

	document.addEventListener( 'focusin', eventListener );
}

// Skins may decide to disable this behaviour or use an alternative module.
if ( config.search ) {
	loadSearchModule( 'mediawiki.searchSuggest' );
}

try {
	// Load the post-edit notification module if a notification has been scheduled.
	// Use `sessionStorage` directly instead of 'mediawiki.storage' to minimize dependencies.
	if ( sessionStorage.getItem( 'mw-PostEdit' + mw.config.get( 'wgPageName' ) ) ) {
		mw.loader.load( 'mediawiki.action.view.postEdit' );
	}
} catch ( err ) {}

/**
 * @exports mediawiki.page.ready
 */
module.exports = {
	loadSearchModule,
	/** @type {CheckboxHack} */
	checkboxHack: require( './checkboxHack.js' ),
	/**
	 * A container for displaying elements that overlay the page e.g. dialogs.
	 *
	 * @type {HTMLElement}
	 */
	teleportTarget: teleportTarget.target
};
},"checkboxShift.js":function(require,module,exports){/**
 * Enable checkboxes to be checked or unchecked in a row by clicking one,
 * holding shift and clicking another one.
 *
 * @method checkboxShift
 * @memberof mediawiki.page.module:ready
 * @param {jQuery} $box
 */
module.exports = function ( $box ) {
	var prev;
	// When our boxes are clicked..
	$box.on( 'click', function ( e ) {
		// And one has been clicked before...
		if ( prev && e.shiftKey ) {
			// Check or uncheck this one and all in-between checkboxes,
			// except for disabled ones
			$box
				.slice(
					Math.min( $box.index( prev ), $box.index( e.target ) ),
					Math.max( $box.index( prev ), $box.index( e.target ) ) + 1
				)
				.filter( function () {
					return !this.disabled && this.checked !== e.target.checked;
				} )
				.prop( 'checked', e.target.checked )
				// Since the state change is a consequence of direct user action,
				// fire the 'change' event (see T313077).
				.trigger( 'change' );
		}
		// Either way, remember this as the last clicked one
		prev = e.target;
	} );
};
},"checkboxHack.js":function(require,module,exports){/**
 * Utility library for managing components using the [CSS checkbox hack]{@link https://css-tricks.com/the-checkbox-hack/}.
 * To access call ```require('mediawiki.page.ready').checkboxHack```.
 *
 * The checkbox hack works without JavaScript for graphical user-interface users, but relies on
 * enhancements to work well for screen reader users. This module provides required a11y
 * interactivity for updating the `aria-expanded` accessibility state, and optional enhancements
 * for avoiding the distracting focus ring when using a pointing device, and target dismissal on
 * focus loss or external click.
 *
 * The checkbox hack is a prevalent pattern in MediaWiki similar to disclosure widgets[0]. Although
 * dated and out-of-fashion, it's surprisingly flexible allowing for both `details` / `summary`-like
 * patterns, menu components, and more complex structures (to be used sparingly) where the toggle
 * button and target are in different parts of the Document without an enclosing element, so long as
 * they can be described as a sibling to the input. It's complicated and frequent enough to warrant
 * single implementation.
 *
 * In time, proper disclosure widgets should replace checkbox hacks. However, the second pattern has
 * no equivalent so the checkbox hack may have a continued use case for some time to come.
 *
 * When the abstraction is leaky, the underlying implementation is simpler than anything built to
 * hide it. Attempts to abstract the functionality for the second pattern failed so all related code
 * celebrates the implementation as directly as possible.
 *
 * All the code assumes that when the input is checked, the target is in an expanded state.
 *
 * Consider the disclosure widget pattern first mentioned:
 *
 * ```html
 * <details>                                              <!-- Container -->
 *     <summary>Click to expand navigation menu</summary> <!-- Button -->
 *     <ul>                                               <!-- Target -->
 *         <li>Main page</li>
 *         <li>Random article</li>
 *         <li>Donate to Wikipedia</li>
 *     </ul>
 * </details>
 * ```
 *
 * Which is represented verbosely by a checkbox hack as such:
 *
 * ```html
 * <div>                                                 <!-- Container -->
 *     <input                                            <!-- Visually hidden checkbox -->
 *         type="checkbox"
 *         id="sidebar-checkbox"
 *         class="mw-checkbox-hack-checkbox"
 *         {{#visible}}checked{{/visible}}
 *         role="button"
 *         aria-labelledby="sidebar-button"
 *         aria-expanded="true||false"
 *         aria-haspopup="true">                         <!-- Optional attribute -->
 *     <label                                            <!-- Button -->
 *         id="sidebar-button"
 *         class="mw-checkbox-hack-button"
 *         for="sidebar-checkbox"
 *         aria-hidden="true">
 *         Click to expand navigation menu
 *     </label>
 *     <ul id="sidebar" class="mw-checkbox-hack-target"> <!-- Target -->
 *         <li>Main page</li>
 *         <li>Random article</li>
 *         <li>Donate to Wikipedia</li>
 *     </ul>
 * </div>
 * ```
 *
 * Where the checkbox is the input, the label is the button, and the target is the unordered list.
 * `aria-haspopup` is an optional attribute that can be applied when dealing with popup elements (i.e. menus).
 *
 * Note that while the label acts as a button for visual users (i.e. it's usually styled as a button and is clicked),
 * the checkbox is what's actually interacted with for keyboard and screenreader users. Many of the HTML attributes
 * and JS enhancements serve to give the checkbox the behavior and semantics of a button.
 * For this reason any hover/focus/active state styles for the button should be applied based on the checkbox state
 * (i.e. https://github.com/wikimedia/mediawiki/blob/master/resources/src/mediawiki.ui.button/button.less#L90)
 *
 * Consider the disparate pattern:
 *
 * ```html
 * <!-- ... -->
 * <!-- The only requirement is that the button and target can be described as a sibling to the
 *      checkbox. -->
 * <input
 *     type="checkbox"
 *     id="sidebar-checkbox"
 *     class="mw-checkbox-hack-checkbox"
 *     {{#visible}}checked{{/visible}}
 *     role="button"
 *     aria-labelledby="sidebar-button"
 *     aria-expanded="true||false"
 *     aria-haspopup="true">
 * <!-- ... -->
 * <label
 *     id="sidebar-button"
 *     class="mw-checkbox-hack-button"
 *     for="sidebar-checkbox"
 *     aria-hidden="true">
 *     Toggle navigation menu
 * </label>
 * <!-- ... -->
 * <ul id="sidebar" class="mw-checkbox-hack-target">
 *     <li>Main page</li>
 *     <li>Random article</li>
 *     <li>Donate to Wikipedia</li>
 * </ul>
 * <!-- ... -->
 * ```
 *
 * Which is the same as the disclosure widget but without the enclosing container and the input only
 * needs to be a preceding sibling of the button and target. It's possible to bend the checkbox hack
 * further to allow the button and target to be at an arbitrary depth so long as a parent can be
 * described as a succeeding sibling of the input, but this requires a mixin implementation that
 * duplicates the rules for each relation selector.
 *
 * Exposed APIs should be considered stable.
 *
 * Accompanying checkbox hack styles are tracked in T252774.
 *
 * [0]: https://developer.mozilla.org/docs/Web/HTML/Element/details
 *
 * @namespace CheckboxHack
 */
/**
 * Revise the button's `aria-expanded` state to match the checked state.
 *
 * @memberof CheckboxHack
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 */
function updateAriaExpanded( checkbox, button ) {
	if ( button ) {
		mw.log.warn( '[1.38] The button parameter in updateAriaExpanded is deprecated, aria-expanded will be applied to the checkbox going forward. View the updated checkbox hack documentation for more details.' );
		button.setAttribute( 'aria-expanded', checkbox.checked.toString() );
		return;
	}

	checkbox.setAttribute( 'aria-expanded', checkbox.checked.toString() );
}

/**
 * Set the checked state and fire the 'input' event.
 * Programmatic changes to checkbox.checked do not trigger an input or change event.
 * The input event in turn will call updateAriaExpanded().
 *
 * setCheckedState() is called when a user event on some element other than the checkbox
 * should result in changing the checkbox state.
 *
 * Per https://html.spec.whatwg.org/multipage/indices.html#event-input
 * Input event is fired at controls when the user changes the value.
 * Per https://html.spec.whatwg.org/multipage/input.html#checkbox-state-(type=checkbox):event-input
 * Fire an event named input at the element with the bubbles attribute initialized to true.
 *
 * https://html.spec.whatwg.org/multipage/indices.html#event-change
 * For completeness the 'change' event should be fired too,
 * however we make no use of the 'change' event,
 * nor expect it to be used, thus firing it
 * would be unnecessary load.
 *
 * @param {HTMLInputElement} checkbox
 * @param {boolean} checked
 * @ignore
 */
function setCheckedState( checkbox, checked ) {
	checkbox.checked = checked;
	// Chrome and Firefox sends the builtin Event with .bubbles == true and .composed == true.
	/** @type {Event} */
	var e;
	if ( typeof Event === 'function' ) {
		e = new Event( 'input', { bubbles: true, composed: true } );
	} else {
		// IE 9-11, FF 6-10, Chrome 9-14, Safari 5.1, Opera 11.5, Android 3-4.3
		e = document.createEvent( 'CustomEvent' );
		if ( !e ) {
			return;
		}
		e.initCustomEvent( 'input', true /* canBubble */, false, false );
	}
	checkbox.dispatchEvent( e );
}

/**
 * Returns true if the Event's target is an inclusive descendant of any the checkbox hack's
 * constituents (checkbox, button, or target), and false otherwise.
 *
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @param {Node} target
 * @param {Event} event
 * @return {boolean}
 * @ignore
 */
function containsEventTarget( checkbox, button, target, event ) {
	return event.target instanceof Node && (
		checkbox.contains( event.target ) ||
		button.contains( event.target ) ||
		target.contains( event.target )
	);
}

/**
 * Dismiss the target when event is outside the checkbox, button, and target.
 * In simple terms this closes the target (menu, typically) when clicking somewhere else.
 *
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @param {Node} target
 * @param {Event} event
 * @ignore
 */
function dismissIfExternalEventTarget( checkbox, button, target, event ) {
	if ( checkbox.checked && !containsEventTarget( checkbox, button, target, event ) ) {
		setCheckedState( checkbox, false );
	}
}

/**
 * Update the `aria-expanded` attribute based on checkbox state (target visibility) changes.
 *
 * @memberof CheckboxHack
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bindUpdateAriaExpandedOnInput( checkbox, button ) {
	if ( button ) {
		mw.log.warn( '[1.38] The button parameter in bindUpdateAriaExpandedOnInput is deprecated, aria-expanded will be applied to the checkbox going forward. View the updated checkbox hack documentation for more details.' );
	}

	var listener = updateAriaExpanded.bind( undefined, checkbox, button );
	// Whenever the checkbox state changes, update the `aria-expanded` state.
	checkbox.addEventListener( 'input', listener );

	return function () {
		checkbox.removeEventListener( 'input', listener );
	};
}

/**
 * Manually change the checkbox state to avoid a focus change when using a pointing device.
 *
 * @memberof CheckboxHack
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bindToggleOnClick( checkbox, button ) {
	function listener( event ) {
		// Do not allow the browser to handle the checkbox. Instead, manually toggle it which does
		// not alter focus.
		event.preventDefault();
		setCheckedState( checkbox, !checkbox.checked );
	}
	button.addEventListener( 'click', listener, true );

	return function () {
		button.removeEventListener( 'click', listener, true );
	};
}

/**
 * Manually change the checkbox state when the button is focused and SPACE is pressed.
 *
 * @deprecated Use `bindToggleOnEnter` instead.
 * @memberof CheckboxHack
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bindToggleOnSpaceEnter( checkbox, button ) {
	mw.log.warn( '[1.38] bindToggleOnSpaceEnter is deprecated. Use `bindToggleOnEnter` instead.' );

	function isEnterOrSpace( /** @type {KeyboardEvent} */ event ) {
		return event.key === ' ' || event.key === 'Enter';
	}

	function onKeydown( /** @type {KeyboardEvent} */ event ) {
		// Only handle SPACE and ENTER.
		if ( !isEnterOrSpace( event ) ) {
			return;
		}
		// Prevent the browser from scrolling when pressing space. The browser will
		// try to do this unless the "button" element is a button or a checkbox.
		// Depending on the actual "button" element, this also possibly prevents a
		// native click event from being triggered so we programatically trigger a
		// click event in the keyup handler.
		event.preventDefault();
	}

	function onKeyup( /** @type {KeyboardEvent} */ event ) {
		// Only handle SPACE and ENTER.
		if ( !isEnterOrSpace( event ) ) {
			return;
		}

		// A native button element triggers a click event when the space or enter
		// keys are pressed. Since the passed in "button" may or may not be a
		// button, programmatically trigger a click event to make it act like a
		// button.
		button.click();
	}

	button.addEventListener( 'keydown', onKeydown );
	button.addEventListener( 'keyup', onKeyup );

	return function () {
		button.removeEventListener( 'keydown', onKeydown );
		button.removeEventListener( 'keyup', onKeyup );
	};
}

/**
 * Manually change the checkbox state when the button is focused and Enter is pressed.
 *
 * @memberof CheckboxHack
 * @param {HTMLInputElement} checkbox
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bindToggleOnEnter( checkbox ) {
	function onKeyup( /** @type {KeyboardEvent} */ event ) {
		// Only handle ENTER.
		if ( event.key !== 'Enter' ) {
			return;
		}

		setCheckedState( checkbox, !checkbox.checked );
	}

	checkbox.addEventListener( 'keyup', onKeyup );

	return function () {
		checkbox.removeEventListener( 'keyup', onKeyup );
	};
}

/**
 * Dismiss the target when clicking elsewhere and update the `aria-expanded` attribute based on
 * checkbox state (target visibility).
 *
 * @memberof CheckboxHack
 * @param {window} window
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @param {Node} target
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bindDismissOnClickOutside( window, checkbox, button, target ) {
	var listener = dismissIfExternalEventTarget.bind( undefined, checkbox, button, target );
	window.addEventListener( 'click', listener, true );

	return function () {
		window.removeEventListener( 'click', listener, true );
	};
}

/**
 * Dismiss the target when focusing elsewhere and update the `aria-expanded` attribute based on
 * checkbox state (target visibility).
 *
 * @param {window} window
 * @param {HTMLInputElement} checkbox
 * @param {HTMLElement} button
 * @param {Node} target
 * @return {function(): void} Cleanup function that removes the added event listeners.
 * @memberof CheckboxHack
 */
function bindDismissOnFocusLoss( window, checkbox, button, target ) {
	// If focus is given to any element outside the target, dismiss the target. Setting a focusout
	// listener on the target would be preferable, but this interferes with the click listener.
	var listener = dismissIfExternalEventTarget.bind( undefined, checkbox, button, target );
	window.addEventListener( 'focusin', listener, true );

	return function () {
		window.removeEventListener( 'focusin', listener, true );
	};
}

/**
 * Dismiss the target when clicking on a link to prevent the target from being open
 * when navigating to a new page.
 *
 * @param {HTMLInputElement} checkbox
 * @param {Node} target
 * @return {function(): void} Cleanup function that removes the added event listeners.
 * @memberof CheckboxHack
 */
function bindDismissOnClickLink( checkbox, target ) {
	function dismissIfClickLinkEvent( event ) {
		// Handle clicks to links and link children elements
		if ( event.target.nodeName === 'A' || event.target.parentNode.nodeName === 'A' ) {
			setCheckedState( checkbox, false );
		}
	}
	target.addEventListener( 'click', dismissIfClickLinkEvent );

	return function () {
		target.removeEventListener( 'click', dismissIfClickLinkEvent );
	};
}

/**
 * Dismiss the target when clicking or focusing elsewhere and update the `aria-expanded` attribute
 * based on checkbox state (target visibility) changes made by **the user.** When tapping the button
 * itself, clear the focus outline.
 *
 * This function calls the other bind* functions and is the only expected interaction for most use
 * cases. It's constituents are provided distinctly for the other use cases.
 *
 * @memberof CheckboxHack
 * @param {window} window
 * @param {HTMLInputElement} checkbox The underlying hidden checkbox that controls target
 *   visibility.
 * @param {HTMLElement} button The visible label icon associated with the checkbox. This button
 *   toggles the state of the underlying checkbox.
 * @param {Node} target The Node to toggle visibility of based on checkbox state.
 * @return {function(): void} Cleanup function that removes the added event listeners.
 */
function bind( window, checkbox, button, target ) {
	var cleanups = [
		bindUpdateAriaExpandedOnInput( checkbox ),
		bindToggleOnClick( checkbox, button ),
		bindToggleOnEnter( checkbox ),
		bindDismissOnClickOutside( window, checkbox, button, target ),
		bindDismissOnFocusLoss( window, checkbox, button, target ),
		bindDismissOnClickLink( checkbox, target )
	];

	return function () {
		cleanups.forEach( function ( cleanup ) {
			cleanup();
		} );
	};
}

module.exports = {
	updateAriaExpanded,
	bindUpdateAriaExpandedOnInput,
	bindToggleOnClick,
	bindToggleOnSpaceEnter,
	bindToggleOnEnter,
	bindDismissOnClickOutside,
	bindDismissOnFocusLoss,
	bindDismissOnClickLink,
	bind
};
},"teleportTarget.js":function(require,module,exports){/**
 * @private
 * @class mw.plugin.page.ready
 */

const ID = 'mw-teleport-target';

const target = document.createElement( 'div' );
target.id = ID;

/**
 * Manages a dedicated container element for modals and dialogs.
 *
 * This creates an empty div and attaches it to the end of the body element.
 * This div can be used by Codex Dialogs and similar components that
 * may need to be displayed on the page.
 *
 * Skins should apply body content styles to this element so that
 * dialogs will use the same styles (font sizes, etc).
 *
 * @return {Object}
 * @return {HTMLDivElement} return.target The div element
 * @return {Function} return.attach Call this function to attach the div to the <body>
 */
module.exports = {
	target,
	attach() {
		document.body.appendChild( target );
	}
};
},"config.json":{
    "search": false,
    "collapsible": true,
    "sortable": true,
    "selectorLogoutLink": "#pt-logout a[data-mw=\"interface\"]"
}}}];});
mw.loader.state({
    "mediawiki.page.ready": "ready"
});