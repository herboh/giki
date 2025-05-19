mw.loader.impl(function(){return["mediawiki.cookie@",{"main":"index.js","files":{"index.js":function(require,module,exports){'use strict';

var config = require( './config.json' ),
	defaults = {
		prefix: config.prefix,
		domain: config.domain,
		path: config.path,
		expires: config.expires,
		secure: false,
		sameSite: '',
		sameSiteLegacy: config.sameSiteLegacy
	},
	jar = require( './jar.js' );

// define jQuery Cookie methods
require( './jquery.js' );

/**
 * Manage cookies in a way that is syntactically and functionally similar
 * to the `WebRequest#getCookie` and `WebResponse#setcookie` methods in PHP.
 * Provided by the mediawiki.cookie ResourceLoader module.
 *
 * @author Sam Smith <samsmith@wikimedia.org>
 * @author Matthew Flaschen <mflaschen@wikimedia.org>
 *
 * @namespace mw.cookie
 * @example
 *   mw.loader.using( 'mediawiki.cookie' ).then( () => {
 *     mw.cookie.set('hello', 'world' );
 *   })
 */
mw.cookie = {
	/**
	 * Set or delete a cookie.
	 *
	 * **Note:** If explicitly passing `null` or `undefined` for an options key,
	 * that will override the default. This is natural in JavaScript, but noted
	 * here because it is contrary to MediaWiki's `WebResponse#setcookie()` method
	 * in PHP.
	 *
	 * When using this for persistent storage of identifiers (e.g. for tracking
	 * sessions), be aware that persistence may vary slightly across browsers and
	 * browser versions, and can be affected by a number of factors such as
	 * storage limits (cookie eviction) and session restore features.
	 *
	 * Without an expiry, this creates a session cookie. In a browser, session cookies persist
	 * for the lifetime of the browser *process*. Including across tabs, page views, and windows,
	 * until the browser itself is *fully* closed, or until the browser clears all storage for
	 * a given website. An exception to this is if the user evokes a "restore previous
	 * session" feature that some browsers have.
	 *
	 * @param {string} key
	 * @param {string|null} value Value of cookie. If `value` is `null` then this method will
	 *   instead remove a cookie by name of `key`.
	 * @param {mw.cookie.CookieOptions|Date|number} [options] Options object, or expiry date
	 */

	set: function ( key, value, options ) {
		var prefix, date, sameSiteLegacy;

		// The 'options' parameter may be a shortcut for the expiry.
		if ( arguments.length > 2 && ( !options || options instanceof Date || typeof options === 'number' ) ) {
			options = { expires: options };
		}
		// Apply defaults
		options = Object.assign( {}, defaults, options );

		// Don't pass invalid option to jar.cookie
		prefix = options.prefix;
		delete options.prefix;

		if ( !options.expires ) {
			// Session cookie (null or zero)
			// Normalize to absent (undefined) for jar.cookie.
			delete options.expires;
		} else if ( typeof options.expires === 'number' ) {
			// Lifetime in seconds
			date = new Date();
			date.setTime( Number( date ) + ( options.expires * 1000 ) );
			options.expires = date;
		}

		sameSiteLegacy = options.sameSiteLegacy;
		delete options.sameSiteLegacy;

		if ( value !== null ) {
			value = String( value );
		}

		jar.cookie( prefix + key, value, options );
		if ( sameSiteLegacy && options.sameSite && options.sameSite.toLowerCase() === 'none' ) {
			// Make testing easy by not changing the object passed to the first jar.cookie call
			options = Object.assign( {}, options );
			delete options.sameSite;
			jar.cookie( prefix + 'ss0-' + key, value, options );
		}
	},

	/**
	 * Get the value of a cookie.
	 *
	 * @param {string} key
	 * @param {string} [prefix=wgCookiePrefix] The prefix of the key. If `prefix` is
	 *   `undefined` or `null`, then `wgCookiePrefix` is used
	 * @param {null|string} [defaultValue] defaults to null
	 * @return {string|null} If the cookie exists, then the value of the
	 *   cookie, otherwise `defaultValue`
	 */
	get: function ( key, prefix, defaultValue ) {
		var result;

		if ( prefix === undefined || prefix === null ) {
			prefix = defaults.prefix;
		}

		// Was defaultValue omitted?
		if ( arguments.length < 3 ) {
			defaultValue = null;
		}

		result = jar.cookie( prefix + key );

		return result !== null ? result : defaultValue;
	},

	/**
	 * Get the value of a SameSite=None cookie, using the legacy ss0- cookie if needed.
	 *
	 * @param {string} key
	 * @param {string} [prefix=wgCookiePrefix] The prefix of the key. If `prefix` is
	 *   `undefined` or `null`, then `wgCookiePrefix` is used
	 * @param {null|string} [defaultValue]
	 * @return {string|null} If the cookie exists, then the value of the
	 *   cookie, otherwise `defaultValue`
	 */
	getCrossSite: function ( key, prefix, defaultValue ) {
		var value;

		value = this.get( key, prefix, null );
		if ( value === null ) {
			value = this.get( 'ss0-' + key, prefix, null );
		}
		if ( value === null ) {
			value = defaultValue;
		}
		return value;
	}
};

if ( window.QUnit ) {
	module.exports = {
		jar,
		setDefaults: function ( value ) {
			var prev = defaults;
			defaults = value;
			return prev;
		}
	};
}
},"jar.js":function(require,module,exports){/**
 * Cookie Plugin
 * Based on https://github.com/carhartl/jquery-cookie
 *
 * Copyright 2013 Klaus Hartl
 * Released under the MIT license
 *
 * Now forked by MediaWiki.
 *
 * @private
 * @class mw.cookie.jar
 */
( function () {

	var pluses = /\+/g;
	var config, cookie;

	function raw( s ) {
		return s;
	}

	function decoded( s ) {
		try {
			return unRfc2068( decodeURIComponent( s.replace( pluses, ' ' ) ) );
		} catch ( e ) {
			// If the cookie cannot be decoded this should not throw an error.
			// See T271838.
			return '';
		}
	}

	function unRfc2068( value ) {
		if ( value.indexOf( '"' ) === 0 ) {
			// This is a quoted cookie as according to RFC2068, unescape
			value = value.slice( 1, -1 ).replace( /\\"/g, '"' ).replace( /\\\\/g, '\\' );
		}
		return value;
	}

	function fromJSON( value ) {
		return config.json ? JSON.parse( value ) : value;
	}

	/**
	 * Get, set, or remove a cookie.
	 *
	 * @ignore
	 * @param {string} [key] Cookie name or (when getting) omit to return an object with all
	 *  current cookie keys and values.
	 * @param {string|null} [value] Cookie value to set. If `null`, this method will remove the cookie.
	 *  If omited, this method will get and return the current value.
	 * @param {mw.cookie.CookieOptions} [options]
	 * @return {string|Object} The current value (if getting a cookie), or an internal `document.cookie`
	 *  expression (if setting or removing).
	 */
	config = cookie = function ( key, value, options ) {

		// write
		if ( value !== undefined ) {
			options = Object.assign( {}, config.defaults, options );

			if ( value === null ) {
				options.expires = -1;
			}

			if ( typeof options.expires === 'number' ) {
				var days = options.expires, t = options.expires = new Date();
				t.setDate( t.getDate() + days );
			}

			value = config.json ? JSON.stringify( value ) : String( value );

			try {
				return ( document.cookie = [
					encodeURIComponent( key ), '=', config.raw ? value : encodeURIComponent( value ),
					options.expires ? '; expires=' + options.expires.toUTCString() : '', // use expires attribute, max-age is not supported by IE
					options.path ? '; path=' + options.path : '',
					options.domain ? '; domain=' + options.domain : '',
					options.secure ? '; secure' : '',
					// PATCH: handle SameSite flag --tgr
					options.sameSite ? '; samesite=' + options.sameSite : ''
				].join( '' ) );
			} catch ( e ) {
				// Fail silently if the document is not allowed to access cookies.
				return '';
			}
		}

		// read
		var decode = config.raw ? raw : decoded;
		var cookies;
		try {
			cookies = document.cookie.split( '; ' );
		} catch ( e ) {
			// Fail silently if the document is not allowed to access cookies.
			cookies = [];
		}
		var result = key ? null : {};
		for ( var i = 0, l = cookies.length; i < l; i++ ) {
			var parts = cookies[ i ].split( '=' );
			var name = decode( parts.shift() );
			var s = decode( parts.join( '=' ) );

			if ( key && key === name ) {
				result = fromJSON( s );
				break;
			}

			if ( !key ) {
				result[ name ] = fromJSON( s );
			}
		}

		return result;
	};

	config.defaults = {};

	/**
	 * Remove a cookie by key.
	 *
	 * @ignore
	 * @param {string} key
	 * @param {mw.cookie.CookieOptions} options
	 * @return {boolean} True if the cookie previously existed
	 */
	function removeCookie( key, options ) {
		if ( cookie( key ) !== null ) {
			cookie( key, null, options );
			return true;
		}
		return false;
	}

	module.exports = {
		cookie,
		removeCookie
	};
}() );
},"jquery.js":function(require,module,exports){var jar = require( './jar.js' );

/**
 * Set a cookie.
 *
 * @memberof jQueryPlugins
 * @method cookie
 * @param {string} [key] Cookie name or (when getting) omit to return an object with all
 *  current cookie keys and values.
 * @param {string|null} [value] Cookie value to set. If `null`, this method will remove the cookie.
 *  If omited, this method will get and return the current value.
 * @param {mw.cookie.CookieOptions} [options]
 * @return {string|Object} The current value (if getting a cookie), or an internal `document.cookie`
 *  expression (if setting or removing).
 * @example
 *    mw.loader.using( 'mediawiki.cookie' ).then( () => {
 *       $.cookie( 'name', 'value', {} );
 *    } );
 */
$.cookie = jar.cookie;

/**
 * Remove a cookie by key.
 *
 * @example
 *    mw.loader.using( 'mediawiki.cookie' ).then( () => {
 *       $.removeCookie( 'name', {} );
 *    } );
 * @memberof jQueryPlugins
 * @method removeCookie
 * @param {string} key
 * @param {mw.cookie.CookieOptions} options
 * @return {boolean} True if the cookie previously existed
 */
$.removeCookie = jar.removeCookie;
},"config.json":{
    "prefix": "enwiki",
    "domain": "",
    "path": "/",
    "expires": 2592000,
    "sameSiteLegacy": false
}}}];});
mw.loader.state({
    "mediawiki.cookie": "ready"
});