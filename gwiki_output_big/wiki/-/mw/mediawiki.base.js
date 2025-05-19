mw.loader.impl(function(){return["mediawiki.base@",{"main":"mediawiki.base.js","files":{"mediawiki.base.js":function(require,module,exports){'use strict';

var slice = Array.prototype.slice;

// Apply site-level data
mw.config.set( require( './config.json' ) );

// Load other files in the package
require( './log.js' );
require( './errorLogger.js' );

/**
 * Object constructor for messages.
 * The constructor is not publicly accessible; use {@link mw.message} instead.
 *
 * @example
 *
 *     var obj, str;
 *     mw.messages.set( {
 *         'hello': 'Hello world',
 *         'hello-user': 'Hello, $1!',
 *         'welcome-user': 'Welcome back to $2, $1! Last visit by $1: $3',
 *         'so-unusual': 'You will find: $1'
 *     } );
 *
 *     obj = mw.message( 'hello' );
 *     mw.log( obj.text() );
 *     // Hello world
 *
 *     obj = mw.message( 'hello-user', 'John Doe' );
 *     mw.log( obj.text() );
 *     // Hello, John Doe!
 *
 *     obj = mw.message( 'welcome-user', 'John Doe', 'Wikipedia', '2 hours ago' );
 *     mw.log( obj.text() );
 *     // Welcome back to Wikipedia, John Doe! Last visit by John Doe: 2 hours ago
 *
 *     // Using mw.msg shortcut, always in "text' format.
 *     str = mw.msg( 'hello-user', 'John Doe' );
 *     mw.log( str );
 *     // Hello, John Doe!
 *
 *     // Different formats
 *     obj = mw.message( 'so-unusual', 'Time "after" <time>' );
 *
 *     mw.log( obj.text() );
 *     // You will find: Time "after" <time>
 *
 *     mw.log( obj.escaped() );
 *     // You will find: Time &quot;after&quot; &lt;time&gt;
 *
 * @class mw.Message
 * @classdesc Describes a translateable text or HTML string. Similar to the Message class in MediaWiki PHP.
 * @param {mw.Map} map Message store
 * @param {string} key
 * @param {Array} [parameters]
 */
function Message( map, key, parameters ) {
	this.map = map;
	this.key = key;
	this.parameters = parameters || [];
}

Message.prototype = {
	/**
	 * Get parsed contents of the message.
	 *
	 * The default parser does simple $N replacements and nothing else.
	 * This may be overridden to provide a more complex message parser.
	 * The primary override is in the mediawiki.jqueryMsg module.
	 *
	 * This function will not be called for nonexistent messages.
	 * For internal use by mediawiki.jqueryMsg only
	 *
	 * @private
	 * @param {string} format
	 * @return {string} Parsed message
	 */
	parser: function ( format ) {
		var text = this.map.get( this.key );
		if (
			mw.config.get( 'wgUserLanguage' ) === 'qqx' &&
			text === '(' + this.key + ')'
		) {
			text = '(' + this.key + '$*)';
		}
		text = mw.format.apply( null, [ text ].concat( this.parameters ) );
		if ( format === 'parse' ) {
			// We don't know how to parse anything, so escape it all
			text = mw.html.escape( text );
		}
		return text;
	},

	/**
	 * Add (does not replace) parameters for `$N` placeholder values.
	 *
	 * @memberof mw.Message
	 * @param {Array} parameters
	 * @return {mw.Message}
	 * @chainable
	 */
	params: function ( parameters ) {
		// Optimization: push all parameter arguments at once. Can't use spread operator
		// `this.parameters.push( ...parameters );` yet, but apply() does the same thing.
		Array.prototype.push.apply( this.parameters, parameters );
		return this;
	},

	/**
	 * Convert message object to a string using the "text"-format .
	 *
	 * This exists for implicit string type casting only.
	 * Do not call this directly. Use mw.Message#text() instead, one of the
	 * other format methods.
	 *
	 * @private
	 * @param {string} [format="text"] Internal parameter. Uses "text" if called
	 *  implicitly through string casting.
	 * @return {string} Message in the given format, or `⧼key⧽` if the key
	 *  does not exist.
	 */
	toString: function ( format ) {
		if ( !this.exists() ) {
			// Use ⧼key⧽ as text if key does not exist
			// Err on the side of safety, ensure that the output
			// is always html safe in the event the message key is
			// missing, since in that case its highly likely the
			// message key is user-controlled.
			// '⧼' is used instead of '<' to side-step any
			// double-escaping issues.
			// (Keep synchronised with Message::toString() in PHP.)
			return '⧼' + mw.html.escape( this.key ) + '⧽';
		}

		if ( !format ) {
			format = 'text';
		}

		if ( format === 'plain' || format === 'text' || format === 'parse' ) {
			return this.parser( format );
		}

		// Format: 'escaped' (including for any invalid format, default to safe escape)
		return mw.html.escape( this.parser( 'escaped' ) );
	},

	/**
	 * Parse message as wikitext and return HTML.
	 *
	 * If jqueryMsg is loaded, this transforms text and parses a subset of supported wikitext
	 * into HTML. Without jqueryMsg, it is equivalent to #escaped.
	 *
	 * @memberof mw.Message
	 * @return {string} String form of parsed message
	 */
	parse: function () {
		return this.toString( 'parse' );
	},

	/**
	 * Return message plainly.
	 *
	 * This substitutes parameters, but otherwise does not transform the
	 * message content.
	 *
	 * @memberof mw.Message
	 * @return {string} String form of plain message
	 */
	plain: function () {
		return this.toString( 'plain' );
	},

	/**
	 * Format message with text transformations applied.
	 *
	 * If jqueryMsg is loaded, `{{`-transformation is done for supported
	 * magic words such as `{{plural:}}`, `{{gender:}}`, and `{{int:}}`.
	 * Without jqueryMsg, it is equivalent to #plain.
	 *
	 * @memberof mw.Message
	 * @return {string} String form of text message
	 */
	text: function () {
		return this.toString( 'text' );
	},

	/**
	 * Format message and return as escaped text in HTML.
	 *
	 * This is equivalent to the #text format, which is then HTML-escaped.
	 *
	 * @memberof mw.Message
	 * @return {string} String form of html escaped message
	 */
	escaped: function () {
		return this.toString( 'escaped' );
	},

	/**
	 * Check if a message exists. Equivalent to {@link mw.Map#exists}.
	 *
	 * @memberof mw.Message
	 * @return {boolean}
	 */
	exists: function () {
		return this.map.exists( this.key );
	}
};

/**
 * @class mw
 * @singleton
 */

/**
 * Empty object for third-party libraries, for cases where you don't
 * want to add a new global, or the global is bad and needs containment
 * or wrapping.
 *
 * @property {Object}
 */
mw.libs = {};

/**
 * OOUI widgets specific to MediaWiki.
 * Initially empty. To expand the amount of available widgets the `mediawiki.widget` module can be loaded.
 *
 * @namespace mw.widgets
 * @example
 * mw.loader.using('mediawiki.widget').then(() => {
 *   OO.ui.getWindowManager().addWindows( [ new mw.widget.AbandonEditDialog() ] );
 * });
 */
mw.widgets = {};

/**
 * Generates a ResourceLoader report using the
 * {@link mediawiki.inspect.js.html|mediawiki.inspect module}.
 *
 * @memberof mw
 */
mw.inspect = function () {
	var args = arguments;
	// Lazy-load
	mw.loader.using( 'mediawiki.inspect', function () {
		mw.inspect.runReports.apply( mw.inspect, args );
	} );
};

/**
 * Replace `$*` with a list of parameters for `uselang=qqx` support.
 *
 * @private
 * @since 1.33
 * @param {string} formatString Format string
 * @param {Array} parameters Values for $N replacements
 * @return {string} Transformed format string
 */
mw.internalDoTransformFormatForQqx = function ( formatString, parameters ) {
	if ( formatString.indexOf( '$*' ) !== -1 ) {
		var replacement = '';
		if ( parameters.length ) {
			replacement = ': ' + parameters.map( function ( _, i ) {
				return '$' + ( i + 1 );
			} ).join( ', ' );
		}
		return formatString.replace( '$*', replacement );
	}
	return formatString;
};

/**
 * Encode page titles in a way that matches `wfUrlencode` in PHP.
 *
 * @see mw.util#wikiUrlencode
 * @private
 * @param {string} str
 * @return {string}
 */
mw.internalWikiUrlencode = function ( str ) {
	return encodeURIComponent( String( str ) )
		.replace( /'/g, '%27' )
		.replace( /%20/g, '_' )
		.replace( /%3B/g, ';' )
		.replace( /%40/g, '@' )
		.replace( /%24/g, '$' )
		.replace( /%2C/g, ',' )
		.replace( /%2F/g, '/' )
		.replace( /%3A/g, ':' );
};

/**
 * Format a string. Replace $1, $2 ... $N with positional arguments.
 *
 * Used by Message#parser().
 *
 * @memberof mw
 * @since 1.25
 * @param {string} formatString Format string
 * @param {...Mixed} parameters Values for $N replacements
 * @return {string} Formatted string
 */
mw.format = function ( formatString ) {
	var parameters = slice.call( arguments, 1 );
	formatString = mw.internalDoTransformFormatForQqx( formatString, parameters );
	return formatString.replace( /\$(\d+)/g, function ( str, match ) {
		var index = parseInt( match, 10 ) - 1;
		return parameters[ index ] !== undefined ? parameters[ index ] : '$' + match;
	} );
};

// Expose Message constructor
mw.Message = Message;

/**
 * Get a message object.
 *
 * Shortcut for `new mw.Message( mw.messages, key, parameters )`.
 *
 * @memberof mw
 * @see {@link mw.Message}
 * @param {string} key Key of message to get
 * @param {...Mixed} parameters Values for $N replacements
 * @return {mw.Message}
 */
mw.message = function ( key ) {
	var parameters = slice.call( arguments, 1 );
	return new Message( mw.messages, key, parameters );
};

/**
 * Get a message string using the (default) 'text' format.
 *
 * Shortcut for `mw.message( key, parameters... ).text()`.
 *
 * @memberof mw
 * @see {@link mw.Message}
 * @param {string} key Key of message to get
 * @param {...any} parameters Values for $N replacements
 * @return {string}
 */
mw.msg = function () {
	// Shortcut must process text transformations by default
	// if mediawiki.jqueryMsg is loaded. (T46459)
	return mw.message.apply( mw, arguments ).text();
};

/**
 * Convenience method for loading and accessing the
 * {@link module:mw.notification#notify|mw.notification module}.
 *
 * @memberof mw
 * @param {HTMLElement|HTMLElement[]|jQuery|mw.Message|string} message
 * @param {Object} [options] See mw.notification#defaults for the defaults.
 * @return {jQuery.Promise}
 */
mw.notify = function ( message, options ) {
	// Lazy load
	return mw.loader.using( 'mediawiki.notification' ).then( function () {
		return mw.notification.notify( message, options );
	} );
};

var mwLoaderTrack = mw.track;
var trackCallbacks = $.Callbacks( 'memory' );
var trackHandlers = [];

/**
 * Track an analytic event.
 *
 * This method provides a generic means for MediaWiki JavaScript code to capture state
 * information for analysis. Each logged event specifies a string topic name that describes
 * the kind of event that it is. Topic names consist of dot-separated path components,
 * arranged from most general to most specific. Each path component should have a clear and
 * well-defined purpose.
 *
 * Data handlers are registered via `mw.trackSubscribe`, and receive the full set of
 * events that match their subscription, including buffered events that fired before the handler
 * was subscribed.
 *
 * @memberof mw
 * @param {string} topic Topic name
 * @param {Object|number|string} [data] Data describing the event.
 */
mw.track = function ( topic, data ) {
	mwLoaderTrack( topic, data );
	trackCallbacks.fire( mw.trackQueue );
};

/**
 * Register a handler for subset of analytic events, specified by topic.
 *
 * Handlers will be called once for each tracked event, including for any buffered events that
 * fired before the handler was subscribed. The callback is passed a `topic` string, and optional
 * `data` event object. The `this` value for the callback is a plain object with `topic` and
 * `data` properties set to those same values.
 *
 * Example to monitor all topics for debugging:
 *
 *     mw.trackSubscribe( '', console.log );
 *
 * Example to subscribe to any of `foo.*`, e.g. both `foo.bar` and `foo.quux`:
 *
 *     mw.trackSubscribe( 'foo.', console.log );
 *
 * @memberof mw
 * @param {string} topic Handle events whose name starts with this string prefix
 * @param {Function} callback Handler to call for each matching tracked event
 * @param {string} callback.topic
 * @param {Object} [callback.data]
 */
mw.trackSubscribe = function ( topic, callback ) {
	var seen = 0;
	function handler( trackQueue ) {
		for ( ; seen < trackQueue.length; seen++ ) {
			var event = trackQueue[ seen ];
			if ( event.topic.indexOf( topic ) === 0 ) {
				callback.call( event, event.topic, event.data );
			}
		}
	}

	trackHandlers.push( [ handler, callback ] );
	trackCallbacks.add( handler );
};

/**
 * Stop handling events for a particular handler
 *
 * @memberof mw
 * @param {Function} callback
 */
mw.trackUnsubscribe = function ( callback ) {
	trackHandlers = trackHandlers.filter( function ( fns ) {
		if ( fns[ 1 ] === callback ) {
			trackCallbacks.remove( fns[ 0 ] );
			// Ensure the tuple is removed to avoid holding on to closures
			return false;
		}
		return true;
	} );
};

// Fire events from before track() triggered fire()
trackCallbacks.fire( mw.trackQueue );

/**
 * @namespace Hooks
 * @classdesc Registry and firing of events.
 *
 * MediaWiki has various interface components that are extended, enhanced
 * or manipulated in some other way by extensions, gadgets and even
 * in core itself.
 *
 * This framework helps streamlining the timing of when these other
 * code paths fire their plugins (instead of using document-ready,
 * which can and should be limited to firing only once).
 *
 * Features like navigating to other wiki pages, previewing an edit
 * and editing itself – without a refresh – can then retrigger these
 * hooks accordingly to ensure everything still works as expected.
 *
 * Example usage:
 *
 *     mw.hook( 'wikipage.content' ).add( fn ).remove( fn );
 *     mw.hook( 'wikipage.content' ).fire( $content );
 *
 * Handlers can be added and fired for arbitrary event names at any time. The same
 * event can be fired multiple times. The last run of an event is memorized
 * (similar to `$(document).ready` and `$.Deferred().done`).
 * This means if an event is fired, and a handler added afterwards, the added
 * function will be fired right away with the last given event data.
 *
 * Like Deferreds and Promises, the mw.hook object is both detachable and chainable.
 * Thus allowing flexible use and optimal maintainability and authority control.
 * You can pass around the `add` and/or `fire` method to another piece of code
 * without it having to know the event name (or `mw.hook` for that matter).
 *
 *     var h = mw.hook( 'bar.ready' );
 *     new mw.Foo( .. ).fetch( { callback: h.fire } );
 *
 * See available global events below.
 */

var hooks = Object.create( null );

/**
 * Create an instance of {@link Hook}.
 *
 * @example
 *
 *   const hook = mw.hook( 'name' );
 *   hook.add( () => alert( 'Hook was fired' ) );
 *   hook.fire();
 *
 * @param {string} name Name of hook.
 * @return {Hook}
 */
mw.hook = function ( name ) {
	return hooks[ name ] || ( hooks[ name ] = ( function () {
		var memory;
		var fns = [];
		function rethrow( e ) {
			setTimeout( function () {
				throw e;
			} );
		}
		/**
		 * @class Hook
		 * @classdesc An instance of a hook, created via [mw.hook method]{@link mw.hook}.
		 * @global
		 * @hideconstructor
		 */
		return {
			/**
			 * Register a hook handler.
			 *
			 * @param {...Function} handler Function to bind.
			 * @memberof Hook
			 * @return {Hook}
			 */
			add: function () {
				for ( var i = 0; i < arguments.length; i++ ) {
					fns.push( arguments[ i ] );
					if ( memory ) {
						try {
							arguments[ i ].apply( null, memory );
						} catch ( e ) {
							rethrow( e );
						}
					}
				}
				return this;
			},
			/**
			 * Unregister a hook handler.
			 *
			 * @param {...Function} handler Function to unbind.
			 * @memberof Hook
			 * @return {Hook}
			 */
			remove: function () {
				for ( var i = 0; i < arguments.length; i++ ) {
					var j;
					while ( ( j = fns.indexOf( arguments[ i ] ) ) !== -1 ) {
						fns.splice( j, 1 );
					}
				}
				return this;
			},
			/**
			 * Call hook handlers with data.
			 *
			 * @memberof Hook
			 * @param {...any} data
			 * @return {Hook}
			 * @chainable
			 */
			fire: function () {
				for ( var i = 0; i < fns.length; i++ ) {
					try {
						fns[ i ].apply( null, arguments );
					} catch ( e ) {
						rethrow( e );
					}
				}
				memory = slice.call( arguments );
				return this;
			}
		};
	}() ) );
};

/**
 * HTML construction helper functions.
 *
 *     @example
 *
 *     var Html, output;
 *
 *     Html = mw.html;
 *     output = Html.element( 'div', {}, new Html.Raw(
 *         Html.element( 'img', { src: '<' } )
 *     ) );
 *     mw.log( output ); // <div><img src="&lt;"/></div>
 *
 * @namespace mw.html
 * @singleton
 */

function escapeCallback( s ) {
	switch ( s ) {
		case '\'':
			return '&#039;';
		case '"':
			return '&quot;';
		case '<':
			return '&lt;';
		case '>':
			return '&gt;';
		case '&':
			return '&amp;';
	}
}
mw.html = {
	/**
	 * Escape a string for HTML.
	 *
	 * Converts special characters to HTML entities.
	 *
	 *     mw.html.escape( '< > \' & "' );
	 *     // Returns &lt; &gt; &#039; &amp; &quot;
	 *
	 * @param {string} s The string to escape
	 * @return {string} HTML
	 */
	escape: function ( s ) {
		return s.replace( /['"<>&]/g, escapeCallback );
	},

	/**
	 * Create an HTML element string, with safe escaping.
	 *
	 * @param {string} name The tag name.
	 * @param {Object} [attrs] An object with members mapping element names to values
	 * @param {string|mw.html.Raw|null} [contents=null] The contents of the element.
	 *
	 *  - string: Text to be escaped.
	 *  - null: The element is treated as void with short closing form, e.g. `<br/>`.
	 *  - this.Raw: The raw value is directly included.
	 * @return {string} HTML
	 */
	element: function ( name, attrs, contents ) {
		var s = '<' + name;

		if ( attrs ) {
			for ( var attrName in attrs ) {
				var v = attrs[ attrName ];
				// Convert name=true, to name=name
				if ( v === true ) {
					v = attrName;
					// Skip name=false
				} else if ( v === false ) {
					continue;
				}
				s += ' ' + attrName + '="' + this.escape( String( v ) ) + '"';
			}
		}
		if ( contents === undefined || contents === null ) {
			// Self close tag
			s += '/>';
			return s;
		}
		// Regular open tag
		s += '>';
		if ( typeof contents === 'string' ) {
			// Escaped
			s += this.escape( contents );
		} else if ( typeof contents === 'number' || typeof contents === 'boolean' ) {
			// Convert to string
			s += String( contents );
		} else if ( contents instanceof this.Raw ) {
			// Raw HTML inclusion
			s += contents.value;
		} else {
			throw new Error( 'Invalid content type' );
		}
		s += '</' + name + '>';
		return s;
	},

	/**
	 * @classdesc Wrapper object for raw HTML. Can be used with [mw.html.element()]{@link mw.html}.
	 * @class mw.html.Raw
	 * @param {string} value
	 * @property {string} value
	 * @example
	 *
	 *   const raw = new mw.html.Raw( 'Text' );
	 *   mw.html.element( 'div', { class: 'html' }, raw );
	 */
	Raw: function ( value ) {
		this.value = value;
	}
};

/**
 * Schedule a function to run once the page is ready (DOM loaded).
 *
 * @since 1.5.8
 * @memberof window
 * @param {Function} fn
 */
window.addOnloadHook = function ( fn ) {
	$( function () {
		fn();
	} );
};

var loadedScripts = {};

/**
 * Import a script using an absolute URI.
 *
 * @since 1.12.2
 * @memberof window
 * @param {string} url
 * @return {HTMLElement|null} Script tag, or null if it was already imported before
 */
window.importScriptURI = function ( url ) {
	if ( loadedScripts[ url ] ) {
		return null;
	}
	loadedScripts[ url ] = true;
	return mw.loader.addScriptTag( url );
};

/**
 * Import a local JS content page, for use by user scripts and site-wide scripts.
 *
 * Note that if the same title is imported multiple times, it will only
 * be loaded and executed once.
 *
 * @since 1.12.2
 * @memberof window
 * @param {string} title
 * @return {HTMLElement|null} Script tag, or null if it was already imported before
 */
window.importScript = function ( title ) {
	return window.importScriptURI(
		mw.config.get( 'wgScript' ) + '?title=' + mw.internalWikiUrlencode( title ) +
			'&action=raw&ctype=text/javascript'
	);
};

/**
 * Import a local CSS content page, for use by user scripts and site-wide scripts.
 *
 * @since 1.12.2
 * @memberof window
 * @param {string} title
 * @return {HTMLElement} Link tag
 */
window.importStylesheet = function ( title ) {
	return mw.loader.addLinkTag(
		mw.config.get( 'wgScript' ) + '?title=' + mw.internalWikiUrlencode( title ) +
			'&action=raw&ctype=text/css'
	);
};

/**
 * Import a stylesheet using an absolute URI.
 *
 * @since 1.12.2
 * @memberof window
 * @param {string} url
 * @param {string} media
 * @return {HTMLElement} Link tag
 */
window.importStylesheetURI = function ( url, media ) {
	return mw.loader.addLinkTag( url, media );
};

/**
 * Get the names of all registered ResourceLoader modules.
 *
 * @memberof mw.loader
 * @return {string[]}
 */
mw.loader.getModuleNames = function () {
	return Object.keys( mw.loader.moduleRegistry );
};

/**
 * Execute a function after one or more modules are ready.
 *
 * Use this method if you need to dynamically control which modules are loaded
 * and/or when they loaded (instead of declaring them as dependencies directly
 * on your module.)
 *
 * This uses the same loader as for regular module dependencies. This means
 * ResourceLoader will not re-download or re-execute a module for the second
 * time if something else already needed it. And the same browser HTTP cache,
 * and localStorage are checked before considering to fetch from the network.
 * And any on-going requests from other dependencies or using() calls are also
 * automatically re-used.
 *
 * Example of inline dependency on OOjs:
 *
 *     mw.loader.using( 'oojs', function () {
 *         OO.compare( [ 1 ], [ 1 ] );
 *     } );
 *
 * Example of inline dependency obtained via `require()`:
 *
 *     mw.loader.using( [ 'mediawiki.util' ], function ( require ) {
 *         var util = require( 'mediawiki.util' );
 *     } );
 *
 * Since MediaWiki 1.23 this returns a promise.
 *
 * Since MediaWiki 1.28 the promise is resolved with a `require` function.
 *
 * @memberof mw.loader
 * @param {string|Array} dependencies Module name or array of modules names the
 *  callback depends on to be ready before executing
 * @param {Function} [ready] Callback to execute when all dependencies are ready
 * @param {Function} [error] Callback to execute if one or more dependencies failed
 * @return {jQuery.Promise} With a `require` function
 */
mw.loader.using = function ( dependencies, ready, error ) {
	var deferred = $.Deferred();

	// Allow calling with a single dependency as a string
	if ( !Array.isArray( dependencies ) ) {
		dependencies = [ dependencies ];
	}

	if ( ready ) {
		deferred.done( ready );
	}
	if ( error ) {
		deferred.fail( error );
	}

	try {
		// Resolve entire dependency map
		dependencies = mw.loader.resolve( dependencies );
	} catch ( e ) {
		return deferred.reject( e ).promise();
	}

	mw.loader.enqueue(
		dependencies,
		function () {
			deferred.resolve( mw.loader.require );
		},
		deferred.reject
	);

	return deferred.promise();
};

/**
 * Load a script by URL.
 *
 * Example:
 *
 *     mw.loader.getScript(
 *         'https://example.org/x-1.0.0.js'
 *     )
 *         .then( function () {
 *             // Script succeeded. You can use X now.
 *         }, function ( e ) {
 *             // Script failed. X is not avaiable
 *             mw.log.error( e.message ); // => "Failed to load script"
 *         } );
 *     } );
 *
 * @memberof mw.loader
 * @param {string} url Script URL
 * @return {jQuery.Promise} Resolved when the script is loaded
 */
mw.loader.getScript = function ( url ) {
	return $.ajax( url, { dataType: 'script', cache: true } )
		.catch( function () {
			throw new Error( 'Failed to load script' );
		} );
};

// Skeleton user object, extended by the 'mediawiki.user' module.
/**
 * @namespace mw.user
 * @singleton
 * @ignore
 */
mw.user = {
	/**
	 * Map of user preferences and their values.
	 *
	 * @property {mw.Map}
	 */
	options: new mw.Map(),
	/**
	 * Map of retrieved user tokens.
	 *
	 * @property {mw.Map}
	 */
	tokens: new mw.Map()
};

mw.user.options.set( require( './user.json' ) );

// Process callbacks for modern browsers (Grade A) that require modules.
var queue = window.RLQ;
// Replace temporary RLQ implementation from startup.js with the
// final implementation that also processes callbacks that can
// require modules. It must also support late arrivals of
// plain callbacks. (T208093)
window.RLQ = {
	push: function ( entry ) {
		if ( typeof entry === 'function' ) {
			entry();
		} else {
			mw.loader.using( entry[ 0 ], entry[ 1 ] );
		}
	}
};
while ( queue[ 0 ] ) {
	window.RLQ.push( queue.shift() );
}

/**
 * Replace document.write/writeln with basic html parsing that appends
 * to the `<body>` to avoid blanking pages. Added JavaScript will not run.
 *
 * @ignore
 * @deprecated since 1.26
 */
[ 'write', 'writeln' ].forEach( function ( func ) {
	mw.log.deprecate( document, func, function () {
		$( document.body ).append( $.parseHTML( slice.call( arguments ).join( '' ) ) );
	}, 'Use jQuery or mw.loader.load instead.', 'document.' + func );
} );
},"log.js":function(require,module,exports){// This file extends the mw.log skeleton defined in startup/mediawiki.js.
// Code that is not needed by mw.loader is placed here.

/* eslint-disable no-console */

/**
 * Library for logging developer warnings to the JavaScript console.
 *
 * @namespace mw.log
 */

/**
 * Create a function that returns true for the first call from any particular call stack.
 *
 * @private
 * @return {Function}
 * @return {boolean|undefined} return.return True if the caller was not seen before.
 */
function stackSet() {
	// Optimisation: Don't create or compute anything for the common case
	// where deprecations are not triggered.
	var stacks;

	return function isFirst() {
		if ( !stacks ) {
			stacks = new Set();
		}
		var stack = new Error().stack;
		if ( !stacks.has( stack ) ) {
			stacks.add( stack );
			return true;
		}
	};
}

/**
 * Write a message to the browser console's error channel.
 *
 * Most browsers also print a stacktrace when calling this method if the
 * argument is an Error object.
 *
 * @since 1.26
 * @method
 * @param {...Mixed} msg Messages to output to console
 */
mw.log.error = Function.prototype.bind.call( console.error, console );

/**
 * Create a function that logs a deprecation warning when called.
 *
 * @example
 *
 *     var deprecatedNoB = mw.log.makeDeprecated( 'hello_without_b', 'Use of hello without b is deprecated.' );
 *
 *     function hello( a, b ) {
 *       if ( b === undefined ) {
 *         deprecatedNoB();
 *         b = 0;
 *       }
 *       return a + b;
 *     }
 *
 *     hello( 1 );
 *
 * @since 1.38
 * @param {string|null} key Name of the feature for deprecation tracker,
 *  or null for a console-only deprecation.
 * @param {string} msg Deprecation warning.
 * @return {Function}
 */
mw.log.makeDeprecated = function ( key, msg ) {
	var isFirst = stackSet();
	return function maybeLog() {
		if ( isFirst() ) {
			if ( key ) {
				mw.track( 'mw.deprecate', key );
			}
			mw.log.warn( msg );
		}
	};
};

/**
 * Create a property on a host object that, when accessed, will log
 * a deprecation warning to the console.
 *
 * @example
 *
 *    mw.log.deprecate( window, 'myGlobalFn', myGlobalFn );
 *
 *    mw.log.deprecate( Thing, 'old', old, 'Use Other.thing instead', 'Thing.old'  );
 *
 * @param {Object} obj Host object of deprecated property
 * @param {string} key Name of property to create in `obj`
 * @param {Mixed} val The value this property should return when accessed
 * @param {string} [msg] Optional extra text to add to the deprecation warning
 * @param {string} [logName] Name of the feature for deprecation tracker.
 *  Tracking is disabled by default, except for global variables on `window`.
 */
mw.log.deprecate = function ( obj, key, val, msg, logName ) {
	var maybeLog = mw.log.makeDeprecated(
		logName || ( obj === window ? key : null ),
		'Use of "' + ( logName || key ) + '" is deprecated.' + ( msg ? ' ' + msg : '' )
	);
	Object.defineProperty( obj, key, {
		configurable: true,
		enumerable: true,
		get: function () {
			maybeLog();
			return val;
		},
		set: function ( newVal ) {
			maybeLog();
			val = newVal;
		}
	} );
};
},"errorLogger.js":function(require,module,exports){'use strict';

/**
 * Fired via mw.track when an error is not handled by local code and is caught by the
 * window.onerror handler.
 *
 * @ignore
 * @event ~'global.error'
 * @param {string} errorMessage Error message.
 * @param {string} url URL where error was raised.
 * @param {number} line Line number where error was raised.
 * @param {number} [column] Line number where error was raised. Not all browsers
 *   support this.
 * @param {Error|Mixed} [errorObject] The error object. Typically an instance of Error, but
 *   anything (even a primitive value) passed to a throw clause will end up here.
 */

/**
 * Fired via mw.track when an error is logged with mw.errorLogger#logError.
 *
 * @ignore
 * @event ~'error.caught'
 * @param {Error} errorObject The error object
 */

/**
 * Install a `window.onerror` handler that logs errors by notifying both `global.error` and
 * `error.uncaught` topic subscribers that an event has occurred. Note well that the former is
 * done for backwards compatibilty.
 *
 * @private
 * @param {Object} window
 */
function installGlobalHandler( window ) {
	// We will preserve the return value of the previous handler. window.onerror works the
	// opposite way than normal event handlers (returning true will prevent the default
	// action, returning false will let the browser handle the error normally, by e.g.
	// logging to the console), so our fallback old handler needs to return false.
	var oldHandler = window.onerror || function () {
		return false;
	};

	window.onerror = function ( errorMessage, url, line, column, errorObject ) {
		mw.track( 'global.error', {
			errorMessage: errorMessage,
			url: url,
			lineNumber: line,
			columnNumber: column,
			stackTrace: errorObject ? errorObject.stack : '',
			errorObject: errorObject
		} );

		if ( errorObject ) {
			mw.track( 'error.uncaught', errorObject );
		}

		return oldHandler.apply( this, arguments );
	};
}

/**
 * Allows the logging of client errors for later inspections.
 *
 * @namespace mw.errorLogger
 * @singleton
 */
mw.errorLogger = {
	/**
	 * Logs an error by notifying subscribers to the given mw.track() topic
	 * (by default `error.caught`) that an event has occurred.
	 *
	 * @param {Error} error
	 * @param {string} [topic='error.caught'] Error topic. Conventionally in the form
	 *   'error.⧼component⧽' (where ⧼component⧽ identifies the code logging the error at a
	 *   high level; e.g. an extension name).
	 */
	logError: function ( error, topic ) {
		mw.track( topic || 'error.caught', error );
	}
};

if ( window.QUnit ) {
	mw.errorLogger.installGlobalHandler = installGlobalHandler;
} else {
	installGlobalHandler( window );
}
},"config.json":{
    "debug": 1,
    "skin": "vector",
    "stylepath": "/w/skins",
    "wgArticlePath": "/wiki/$1",
    "wgScriptPath": "/w",
    "wgScript": "/w/index.php",
    "wgSearchType": "CirrusSearch",
    "wgVariantArticlePath": false,
    "wgServer": "//en.wikipedia.org",
    "wgServerName": "en.wikipedia.org",
    "wgUserLanguage": "en",
    "wgContentLanguage": "en",
    "wgVersion": "1.42.0-wmf.14",
    "wgFormattedNamespaces": {
        "-2": "Media",
        "-1": "Special",
        "0": "",
        "1": "Talk",
        "2": "User",
        "3": "User talk",
        "4": "Wikipedia",
        "5": "Wikipedia talk",
        "6": "File",
        "7": "File talk",
        "8": "MediaWiki",
        "9": "MediaWiki talk",
        "10": "Template",
        "11": "Template talk",
        "12": "Help",
        "13": "Help talk",
        "14": "Category",
        "15": "Category talk",
        "100": "Portal",
        "101": "Portal talk",
        "118": "Draft",
        "119": "Draft talk",
        "710": "TimedText",
        "711": "TimedText talk",
        "828": "Module",
        "829": "Module talk",
        "2300": "Gadget",
        "2301": "Gadget talk",
        "2302": "Gadget definition",
        "2303": "Gadget definition talk"
    },
    "wgNamespaceIds": {
        "media": -2,
        "special": -1,
        "": 0,
        "talk": 1,
        "user": 2,
        "user_talk": 3,
        "wikipedia": 4,
        "wikipedia_talk": 5,
        "file": 6,
        "file_talk": 7,
        "mediawiki": 8,
        "mediawiki_talk": 9,
        "template": 10,
        "template_talk": 11,
        "help": 12,
        "help_talk": 13,
        "category": 14,
        "category_talk": 15,
        "portal": 100,
        "portal_talk": 101,
        "draft": 118,
        "draft_talk": 119,
        "timedtext": 710,
        "timedtext_talk": 711,
        "module": 828,
        "module_talk": 829,
        "gadget": 2300,
        "gadget_talk": 2301,
        "gadget_definition": 2302,
        "gadget_definition_talk": 2303,
        "wt": 5,
        "wp": 4,
        "image": 6,
        "image_talk": 7,
        "project": 4,
        "project_talk": 5
    },
    "wgContentNamespaces": [
        0
    ],
    "wgSiteName": "Wikipedia",
    "wgDBname": "enwiki",
    "wgWikiID": "enwiki",
    "wgCaseSensitiveNamespaces": [
        2300,
        2301,
        2302,
        2303
    ],
    "wgCommentCodePointLimit": 500,
    "wgExtensionAssetsPath": "/w/extensions",
    "wgUrlProtocols": "bitcoin\\:|ftp\\:\\/\\/|ftps\\:\\/\\/|geo\\:|git\\:\\/\\/|gopher\\:\\/\\/|http\\:\\/\\/|https\\:\\/\\/|irc\\:\\/\\/|ircs\\:\\/\\/|magnet\\:|mailto\\:|matrix\\:|mms\\:\\/\\/|news\\:|nntp\\:\\/\\/|redis\\:\\/\\/|sftp\\:\\/\\/|sip\\:|sips\\:|sms\\:|ssh\\:\\/\\/|svn\\:\\/\\/|tel\\:|telnet\\:\\/\\/|urn\\:|worldwind\\:\\/\\/|xmpp\\:|\\/\\/",
    "wgActionPaths": {},
    "wgTranslateNumerals": true,
    "wgExtraSignatureNamespaces": [
        4,
        12
    ],
    "wgLegalTitleChars": " %!\"$\u0026'()*,\\-./0-9:;=?@A-Z\\\\\\^_`a-z~+\\u0080-\\uFFFF",
    "wgIllegalFileChars": ":/\\\\",
    "wgCentralNoticeActiveBannerDispatcher": "//meta.wikimedia.org/w/index.php?title=Special:BannerLoader",
    "wgCentralSelectedBannerDispatcher": "//meta.wikimedia.org/w/index.php?title=Special:BannerLoader",
    "wgCentralBannerRecorder": "//en.wikipedia.org/beacon/impression",
    "wgCentralNoticeSampleRate": 0.01,
    "wgCentralNoticeImpressionEventSampleRate": 0,
    "wgNoticeNumberOfBuckets": 4,
    "wgNoticeBucketExpiry": 7,
    "wgNoticeNumberOfControllerBuckets": 2,
    "wgNoticeCookieDurations": {
        "close": 604800,
        "donate": 21600000
    },
    "wgNoticeHideUrls": [
        "//en.wikipedia.org/w/index.php?title=Special:HideBanners",
        "//meta.wikimedia.org/w/index.php?title=Special:HideBanners",
        "//commons.wikimedia.org/w/index.php?title=Special:HideBanners",
        "//species.wikimedia.org/w/index.php?title=Special:HideBanners",
        "//en.wikibooks.org/w/index.php?title=Special:HideBanners",
        "//en.wikiquote.org/w/index.php?title=Special:HideBanners",
        "//en.wikisource.org/w/index.php?title=Special:HideBanners",
        "//en.wikinews.org/w/index.php?title=Special:HideBanners",
        "//en.wikiversity.org/w/index.php?title=Special:HideBanners",
        "//www.mediawiki.org/w/index.php?title=Special:HideBanners"
    ],
    "wgCentralNoticeMaxCampaignFallback": 5,
    "wgCentralNoticePerCampaignBucketExtension": 30,
    "wgCiteVisualEditorOtherGroup": false,
    "wgCiteResponsiveReferences": true,
    "wgCiteBookReferencing": false,
    "wgCirrusSearchFeedbackLink": false,
    "wgCodeMirrorLineNumberingNamespaces": [
        10
    ],
    "wgMultimediaViewer": {
        "infoLink": "https://mediawiki.org/wiki/Special:MyLanguage/Extension:Media_Viewer/About",
        "discussionLink": "https://mediawiki.org/wiki/Special:MyLanguage/Extension_talk:Media_Viewer/About",
        "helpLink": "https://mediawiki.org/wiki/Special:MyLanguage/Help:Extension:Media_Viewer",
        "useThumbnailGuessing": true,
        "imageQueryParameter": false,
        "recordVirtualViewBeaconURI": "/beacon/media",
        "extensions": {
            "jpg": "default",
            "jpeg": "default",
            "gif": "default",
            "svg": "default",
            "png": "default",
            "tiff": "default",
            "tif": "default",
            "webp": "default",
            "stl": "mmv.3d"
        }
    },
    "wgMediaViewer": true,
    "wgPopupsVirtualPageViews": true,
    "wgPopupsGateway": "restbaseHTML",
    "wgPopupsRestGatewayEndpoint": "/api/rest_v1/page/summary/",
    "wgPopupsStatsvSamplingRate": 0.01,
    "wgPopupsTextExtractsIntroOnly": true,
    "wgVisualEditorConfig": {
        "usePageImages": true,
        "usePageDescriptions": true,
        "isBeta": false,
        "disableForAnons": true,
        "preloadModules": [
            "site",
            "user"
        ],
        "namespaces": [
            0,
            2,
            6,
            12,
            14,
            100,
            118
        ],
        "contentModels": {
            "wikitext": "article"
        },
        "pluginModules": [
            "ext.wikihiero.visualEditor",
            "ext.cite.visualEditor",
            "ext.geshi.visualEditor",
            "ext.spamBlacklist.visualEditor",
            "ext.titleblacklist.visualEditor",
            "ext.score.visualEditor",
            "ext.confirmEdit.visualEditor",
            "ext.abuseFilter.visualEditor",
            "ext.CodeMirror.visualEditor",
            "ext.citoid.visualEditor",
            "ext.templateDataGenerator.editPage",
            "ext.math.visualEditor",
            "ext.disambiguator.visualEditor",
            "ext.wikimediaEvents.visualEditor",
            "ext.TemplateSandbox.visualeditor",
            "ext.kartographer.editing",
            "ext.kartographer.visualEditor"
        ],
        "thumbLimits": [
            120,
            150,
            180,
            200,
            220,
            250,
            300,
            400
        ],
        "galleryOptions": {
            "imagesPerRow": 0,
            "imageWidth": 120,
            "imageHeight": 120,
            "captionLength": true,
            "showBytes": true,
            "mode": "traditional",
            "showDimensions": true
        },
        "unsupportedList": {
            "firefox": [
                [
                    "\u003C=",
                    11
                ]
            ],
            "safari": [
                [
                    "\u003C=",
                    6
                ]
            ],
            "opera": [
                [
                    "\u003C",
                    12
                ]
            ]
        },
        "tabPosition": "before",
        "tabMessages": {
            "editsource": "visualeditor-ca-editsource",
            "createsource": "visualeditor-ca-createsource",
            "editlocaldescriptionsource": "visualeditor-ca-editlocaldescriptionsource",
            "createlocaldescriptionsource": "visualeditor-ca-createlocaldescriptionsource",
            "editsection": "editsection",
            "editsectionhint": "editsectionhint",
            "editsectionsource": "visualeditor-ca-editsource-section",
            "editsectionsourcehint": "visualeditor-ca-editsource-section-hint"
        },
        "singleEditTab": true,
        "enableVisualSectionEditing": "mobile",
        "showBetaWelcome": true,
        "allowExternalLinkPaste": false,
        "enableHelpCompletion": true,
        "enableTocWidget": false,
        "enableWikitext": true,
        "useChangeTagging": true,
        "editCheckTagging": true,
        "editCheck": false,
        "namespacesWithSubpages": [
            1,
            2,
            3,
            4,
            5,
            7,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            100,
            101,
            118,
            119,
            828,
            829
        ],
        "specialBooksources": "Special:BookSources",
        "rebaserUrl": false,
        "feedbackApiUrl": false,
        "feedbackTitle": false,
        "sourceFeedbackTitle": false,
        "transclusionDialogNewSidebar": true,
        "cirrusSearchLookup": true,
        "defaultSortPrefix": "DEFAULTSORT"
    },
    "wgCitoidConfig": {
        "citoidServiceUrl": false,
        "fullRestbaseUrl": "/api/rest_",
        "isbnScannerEnabled": {
            "mobile": true,
            "desktop": false
        },
        "wbFullRestbaseUrl": false
    },
    "wgTemplateWizardConfig": {
        "cirrusSearchLookup": true
    },
    "wgMathEntitySelectorUrl": "https://www.wikidata.org/w/api.php",
    "pageTriageNamespaces": [
        0,
        118
    ],
    "wgPageTriageDraftNamespaceId": 118,
    "wgRelatedArticlesCardLimit": 3,
    "wgULSIMEEnabled": false,
    "wgULSWebfontsEnabled": false,
    "wgULSAnonCanChangeLanguage": false,
    "wgULSImeSelectors": [
        "input:not([type])",
        "input[type=text]",
        "input[type=search]",
        "textarea",
        "[contenteditable]"
    ],
    "wgULSNoImeSelectors": [
        "#wpCaptchaWord",
        ".ace_text-input",
        ".ve-ce-surface-paste",
        ".ve-ce-surface-readOnly [contenteditable]",
        ".ace_editor textarea"
    ],
    "wgULSNoWebfontsSelectors": [
        "#p-lang li.interlanguage-link \u003E a"
    ],
    "wgULSDisplaySettingsInInterlanguage": false,
    "wgULSFontRepositoryBasePath": "/w/extensions/UniversalLanguageSelector/data/fontrepo/fonts/",
    "wgExternalGuidanceMTReferrers": [
        "translate.google.com",
        "translate.googleusercontent.com"
    ],
    "wgExternalGuidanceSiteTemplates": {
        "view": "//$1.wikipedia.org/wiki/$2",
        "action": "//$1.wikipedia.org/w/index.php?title=$2",
        "api": "//$1.wikipedia.org/w/api.php"
    },
    "wgExternalGuidanceDomainCodeMapping": {
        "be-x-old": "be-tarask",
        "bho": "bh",
        "crh-latn": "crh",
        "en-simple": "simple",
        "gsw": "als",
        "lzh": "zh-classical",
        "nan": "zh-min-nan",
        "nb": "no",
        "rup": "roa-rup",
        "sgs": "bat-smg",
        "vro": "fiu-vro",
        "yue": "zh-yue"
    },
    "wgGEUserVariants": [
        "control",
        "oldimpact"
    ],
    "wgGEDefaultUserVariant": "control"
},"user.json":{
    "vector-limited-width": 1,
    "vector-page-tools-pinned": 1,
    "vector-main-menu-pinned": 1,
    "vector-toc-pinned": 1,
    "vector-client-prefs-pinned": 1,
    "vector-font-size": 0,
    "flaggedrevssimpleui": 1,
    "flaggedrevsstable": 0,
    "flaggedrevseditdiffs": true,
    "flaggedrevsviewdiffs": false,
    "flaggedrevswatch": false,
    "globaluserpage": true,
    "rcenhancedfilters-seen-highlight-button-counter": 0,
    "advancedsearch-disable": 0,
    "usebetatoolbar": 1,
    "wikieditor-realtimepreview": 0,
    "usecodemirror": 0,
    "betafeatures-auto-enroll": false,
    "popupsreferencepreviews": 0,
    "popups-reference-previews": "1",
    "visualeditor-autodisable": 0,
    "visualeditor-betatempdisable": 0,
    "visualeditor-editor": "wikitext",
    "visualeditor-enable": 0,
    "visualeditor-hidebetawelcome": 0,
    "visualeditor-hidetabdialog": 0,
    "visualeditor-newwikitext": 0,
    "visualeditor-tabs": "remember-last",
    "mobile-editor": "",
    "math": "mathml",
    "math-popups": "1",
    "echo-subscriptions-web-page-review": true,
    "echo-subscriptions-email-page-review": false,
    "echo-subscriptions-web-login-fail": true,
    "echo-subscriptions-email-login-fail": true,
    "echo-subscriptions-web-login-success": false,
    "echo-subscriptions-email-login-success": true,
    "echo-email-frequency": 0,
    "echo-dont-email-read-notifications": false,
    "echo-subscriptions-web-edit-thank": true,
    "echo-subscriptions-email-edit-thank": false,
    "discussiontools-betaenable": 0,
    "discussiontools-editmode": "",
    "discussiontools-newtopictool": 1,
    "discussiontools-newtopictool-createpage": 1,
    "discussiontools-replytool": 1,
    "discussiontools-sourcemodetoolbar": 1,
    "discussiontools-topicsubscription": 1,
    "discussiontools-autotopicsub": 0,
    "discussiontools-visualenhancements": 1,
    "usecodeeditor": 1,
    "revisionslider-disable": 0,
    "twocolconflict-enabled": 1,
    "eventlogging-display-web": 0,
    "eventlogging-display-console": 0,
    "uls-preferences": "",
    "compact-language-links": 1,
    "echo-subscriptions-web-cx": true,
    "cx": false,
    "cx-enable-entrypoints": true,
    "cx-entrypoint-fd-status": "notshown",
    "cx_campaign_newarticle_shown": false,
    "rcshowwikidata": 0,
    "wlshowwikibase": 0,
    "echo-subscriptions-web-oauth-owner": true,
    "echo-subscriptions-email-oauth-owner": true,
    "echo-subscriptions-web-oauth-admin": true,
    "echo-subscriptions-email-oauth-admin": true,
    "ores-damaging-flag-rc": false,
    "oresDamagingPref": "soft",
    "rcOresDamagingPref": "soft",
    "oresHighlight": false,
    "oresRCHideNonDamaging": false,
    "oresWatchlistHideNonDamaging": false,
    "ipinfo-use-agreement": 0,
    "twl-notified": null,
    "parsermigration": "0",
    "parsermigration-parsoid-readviews": "0",
    "ccmeonemails": 0,
    "date": "default",
    "diffonly": 0,
    "diff-type": "table",
    "disablemail": 0,
    "editfont": "monospace",
    "editondblclick": 0,
    "editsectiononrightclick": 0,
    "email-allow-new-users": 1,
    "enotifminoredits": false,
    "enotifrevealaddr": 0,
    "enotifusertalkpages": 1,
    "enotifwatchlistpages": 0,
    "extendwatchlist": 0,
    "fancysig": 0,
    "forceeditsummary": 0,
    "forcesafemode": 0,
    "gender": "unknown",
    "hidecategorization": 1,
    "hideminor": 0,
    "hidepatrolled": 0,
    "imagesize": 2,
    "minordefault": 0,
    "newpageshidepatrolled": 0,
    "nickname": "",
    "norollbackdiff": 0,
    "prefershttps": 1,
    "previewonfirst": 0,
    "previewontop": 1,
    "pst-cssjs": 1,
    "rcdays": 7,
    "rcenhancedfilters-disable": 0,
    "rclimit": 50,
    "requireemail": 0,
    "search-match-redirect": true,
    "search-special-page": "Search",
    "search-thumbnail-extra-namespaces": true,
    "searchlimit": 20,
    "showhiddencats": false,
    "shownumberswatching": 1,
    "showrollbackconfirmation": 0,
    "skin": "vector-2022",
    "skin-responsive": 1,
    "thumbsize": 4,
    "underline": 2,
    "useeditwarning": 1,
    "uselivepreview": 0,
    "usenewrc": 0,
    "watchcreations": true,
    "watchdefault": 0,
    "watchdeletion": 0,
    "watchlistdays": 3,
    "watchlisthideanons": 0,
    "watchlisthidebots": 0,
    "watchlisthidecategorization": 1,
    "watchlisthideliu": 0,
    "watchlisthideminor": 0,
    "watchlisthideown": 0,
    "watchlisthidepatrolled": 0,
    "watchlistreloadautomatically": 0,
    "watchlistunwatchlinks": 0,
    "watchmoves": 0,
    "watchrollback": 0,
    "watchuploads": 1,
    "wlenhancedfilters-disable": 0,
    "wllimit": 250,
    "wikilove-enabled": 1,
    "echo-cross-wiki-notifications": 1,
    "growthexperiments-addimage-desktop": 1,
    "timecorrection": "System|0",
    "centralnotice-display-campaign-type-advocacy": 1,
    "centralnotice-display-campaign-type-article-writing": 1,
    "centralnotice-display-campaign-type-photography": 1,
    "centralnotice-display-campaign-type-event": 1,
    "centralnotice-display-campaign-type-fundraising": 1,
    "centralnotice-display-campaign-type-governance": 1,
    "centralnotice-display-campaign-type-maintenance": 1,
    "centralnotice-display-campaign-type-special": 1,
    "language": "en",
    "variant": "en",
    "variant-ban": "ban",
    "variant-en": "en",
    "variant-crh": "crh",
    "variant-gan": "gan",
    "variant-iu": "iu",
    "variant-ku": "ku",
    "variant-sh": "sh-latn",
    "variant-shi": "shi",
    "variant-sr": "sr",
    "variant-tg": "tg",
    "variant-tly": "tly",
    "variant-uz": "uz",
    "variant-wuu": "wuu",
    "variant-zh": "zh",
    "searchNs0": 1,
    "searchNs1": 0,
    "searchNs2": 0,
    "searchNs3": 0,
    "searchNs4": 0,
    "searchNs5": 0,
    "searchNs6": 0,
    "searchNs7": 0,
    "searchNs8": 0,
    "searchNs9": 0,
    "searchNs10": 0,
    "searchNs11": 0,
    "searchNs12": 0,
    "searchNs13": 0,
    "searchNs14": 0,
    "searchNs15": 0,
    "searchNs100": 0,
    "searchNs101": 0,
    "searchNs118": 0,
    "searchNs119": 0,
    "searchNs710": 0,
    "searchNs711": 0,
    "searchNs828": 0,
    "searchNs829": 0,
    "searchNs2300": 0,
    "searchNs2301": 0,
    "searchNs2302": 0,
    "searchNs2303": 0,
    "gadget-modrollback": 0,
    "gadget-confirmationRollback-mobile": 1,
    "gadget-removeAccessKeys": 0,
    "gadget-searchFocus": 0,
    "gadget-GoogleTrans": 0,
    "gadget-ImageAnnotator": 0,
    "gadget-imagelinks": 0,
    "gadget-Navigation_popups": 0,
    "gadget-exlinks": 0,
    "gadget-search-new-tab": 0,
    "gadget-PrintOptions": 0,
    "gadget-revisionjumper": 0,
    "gadget-Twinkle": 0,
    "gadget-HideCentralNotice": 0,
    "gadget-ReferenceTooltips": 1,
    "gadget-formWizard": 1,
    "gadget-Prosesize": 0,
    "gadget-find-archived-section": 0,
    "gadget-geonotice": 1,
    "gadget-watchlist-notice": 1,
    "gadget-WatchlistBase": 1,
    "gadget-WatchlistGreenIndicators": 1,
    "gadget-WatchlistGreenIndicatorsMono": 1,
    "gadget-WatchlistChangesBold": 0,
    "gadget-SubtleUpdatemarker": 1,
    "gadget-defaultsummaries": 0,
    "gadget-citations": 0,
    "gadget-DotsSyntaxHighlighter": 0,
    "gadget-HotCat": 0,
    "gadget-wikEdDiff": 0,
    "gadget-ProveIt": 0,
    "gadget-ProveIt-classic": 0,
    "gadget-Shortdesc-helper": 0,
    "gadget-wikEd": 0,
    "gadget-afchelper": 0,
    "gadget-charinsert": 1,
    "gadget-legacyToolbar": 0,
    "gadget-extra-toolbar-buttons": 1,
    "gadget-refToolbar": 1,
    "gadget-EditNoticesOnMobile": 1,
    "gadget-edittop": 0,
    "gadget-UTCLiveClock": 0,
    "gadget-purgetab": 0,
    "gadget-ExternalSearch": 0,
    "gadget-CollapsibleNav": 0,
    "gadget-MenuTabsToggle": 0,
    "gadget-dropdown-menus": 0,
    "gadget-CategoryAboveAll": 0,
    "gadget-addsection-plus": 0,
    "gadget-CommentsInLocalTime": 0,
    "gadget-OldDiff": 0,
    "gadget-NoAnimations": 0,
    "gadget-disablesuggestions": 0,
    "gadget-NoSmallFonts": 0,
    "gadget-topalert": 0,
    "gadget-metadata": 0,
    "gadget-JustifyParagraphs": 0,
    "gadget-righteditlinks": 0,
    "gadget-PrettyLog": 0,
    "gadget-switcher": 1,
    "gadget-SidebarTranslate": 0,
    "gadget-Blackskin": 0,
    "gadget-dark-mode-toggle": 0,
    "gadget-VectorClassic": 0,
    "gadget-widensearch": 0,
    "gadget-DisambiguationLinks": 0,
    "gadget-markblocked": 0,
    "gadget-responsiveContent": 0,
    "gadget-responsiveContentTimeless": 1,
    "gadget-HideInterwikiSearchResults": 0,
    "gadget-XTools-ArticleInfo": 0,
    "gadget-ShowMessageNames": 0,
    "gadget-DebugMode": 0,
    "gadget-contribsrange": 0,
    "gadget-BugStatusUpdate": 0,
    "gadget-RTRC": 0,
    "gadget-script-installer": 0,
    "gadget-XFDcloser": 0,
    "gadget-mobile-sidebar": 0,
    "gadget-addMe": 0,
    "gadget-NewImageThumb": 0,
    "gadget-StickyTableHeaders": 0,
    "gadget-MobileMaps": 0,
    "gadget-ShowJavascriptErrors": 0,
    "gadget-PageDescriptions": 0,
    "gadget-autonum": 0,
    "gadget-MakeMobileCollapsible": 1,
    "gadget-dark-mode": 0,
    "cirrussearch-pref-completion-profile": "fuzzy",
    "multimediaviewer-enable": 1,
    "popups": "0",
    "mf_amc_optin": "0",
    "echo-email-format": "html",
    "echo-subscriptions-email-system": true,
    "echo-subscriptions-web-system": true,
    "echo-subscriptions-push-system": true,
    "echo-subscriptions-email-system-noemail": false,
    "echo-subscriptions-web-system-noemail": true,
    "echo-subscriptions-push-system-noemail": true,
    "echo-subscriptions-email-system-emailonly": false,
    "echo-subscriptions-web-system-emailonly": true,
    "echo-subscriptions-push-system-emailonly": true,
    "echo-subscriptions-email-user-rights": true,
    "echo-subscriptions-web-user-rights": true,
    "echo-subscriptions-push-user-rights": true,
    "echo-subscriptions-email-other": false,
    "echo-subscriptions-web-other": true,
    "echo-subscriptions-push-other": true,
    "echo-subscriptions-email-edit-user-talk": false,
    "echo-subscriptions-web-edit-user-talk": true,
    "echo-subscriptions-push-edit-user-talk": true,
    "echo-subscriptions-email-edit-user-page": false,
    "echo-subscriptions-web-edit-user-page": true,
    "echo-subscriptions-push-edit-user-page": true,
    "echo-subscriptions-email-reverted": false,
    "echo-subscriptions-web-reverted": true,
    "echo-subscriptions-push-reverted": true,
    "echo-subscriptions-email-article-linked": false,
    "echo-subscriptions-web-article-linked": false,
    "echo-subscriptions-push-article-linked": false,
    "echo-subscriptions-email-mention": false,
    "echo-subscriptions-web-mention": true,
    "echo-subscriptions-push-mention": true,
    "echo-subscriptions-email-mention-failure": false,
    "echo-subscriptions-web-mention-failure": false,
    "echo-subscriptions-push-mention-failure": false,
    "echo-subscriptions-email-mention-success": false,
    "echo-subscriptions-web-mention-success": false,
    "echo-subscriptions-push-mention-success": false,
    "echo-subscriptions-email-emailuser": false,
    "echo-subscriptions-web-emailuser": true,
    "echo-subscriptions-push-emailuser": true,
    "echo-subscriptions-email-thank-you-edit": false,
    "echo-subscriptions-web-thank-you-edit": true,
    "echo-subscriptions-push-thank-you-edit": true,
    "echo-subscriptions-push-page-review": true,
    "echo-subscriptions-email-cx": false,
    "echo-subscriptions-push-cx": true,
    "echo-subscriptions-email-ge-mentorship": false,
    "echo-subscriptions-web-ge-mentorship": true,
    "echo-subscriptions-push-ge-mentorship": true,
    "echo-subscriptions-email-ge-newcomer": true,
    "echo-subscriptions-web-ge-newcomer": true,
    "echo-subscriptions-push-ge-newcomer": true,
    "echo-subscriptions-push-login-fail": true,
    "echo-subscriptions-push-login-success": true,
    "echo-subscriptions-push-edit-thank": true,
    "echo-subscriptions-email-dt-subscription": false,
    "echo-subscriptions-web-dt-subscription": true,
    "echo-subscriptions-push-dt-subscription": true,
    "echo-subscriptions-email-dt-subscription-archiving": false,
    "echo-subscriptions-web-dt-subscription-archiving": true,
    "echo-subscriptions-push-dt-subscription-archiving": true,
    "echo-subscriptions-email-wikibase-action": false,
    "echo-subscriptions-web-wikibase-action": true,
    "echo-subscriptions-push-wikibase-action": true,
    "growthexperiments-help-panel-tog-help-panel": false,
    "growthexperiments-homepage-suggestededits-guidance-blue-dot": "{\"vector\":{\"link-recommendation\":true,\"image-recommendation\":true},\"minerva\":{\"link-recommendation\":true,\"image-recommendation\":true}}",
    "growthexperiments-homepage-se-topic-filters-mode": "OR",
    "growthexperiments-mentee-overview-presets": "{\"usersToShow\":10,\"filters\":{\"minedits\":1,\"maxedits\":500}}",
    "growthexperiments-homepage-mentorship-enabled": 1,
    "growthexperiments-mentorship-was-praised": false,
    "growthexperiments-personalized-praise-skipped-until": null,
    "growthexperiments-personalized-praise-settings": "{}",
    "growthexperiments-tour-help-panel": true,
    "growthexperiments-tour-homepage-mentorship": true,
    "growthexperiments-tour-homepage-welcome": true,
    "growthexperiments-tour-homepage-discovery": true,
    "growthexperiments-tour-newimpact-discovery": false
}}}];});
mw.loader.state({
    "mediawiki.base": "ready"
});