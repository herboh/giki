/**
 * This file is where we decide whether to initialise the modern support browser run-time.
 *
 * - Beware: This file MUST parse without errors on even the most ancient of browsers!
 */
/* eslint-disable no-implicit-globals */
/* global $CODE, RLQ:true, NORLQ:true */

/**
 * See <https://www.mediawiki.org/wiki/Compatibility#Browsers>
 *
 * Browsers that pass these checks get served our modern run-time. This includes all Grade A
 * browsers, and some Grade C and Grade X browsers.
 *
 * The following browsers are known to pass these checks:
 * - Chrome 63+
 * - Edge 79+
 * - Opera 50+
 * - Firefox 58+
 * - Safari 11.1+
 * - Mobile Safari 11.2+ (iOS 11+)
 * - Android 5.0+
 *
 * @private
 * @return {boolean} User agent is compatible with MediaWiki JS
 */
function isCompatible() {
	return !!(
		// Ensure DOM Level 4 features (including Selectors API).
		//
		// https://caniuse.com/#feat=queryselector
		'querySelector' in document &&

		// Ensure HTML 5 features (including Web Storage API)
		//
		// https://caniuse.com/#feat=namevalue-storage
		// https://blog.whatwg.org/this-week-in-html-5-episode-30
		'localStorage' in window &&

		// Ensure ES2015 grammar and runtime API (a.k.a. ES6)
		//
		// In practice, Promise.finally is a good proxy for overall ES6 support and
		// rejects most unsupporting browsers in one sweep. The feature itself
		// was specified in ES2018, however.
		// https://caniuse.com/promise-finally
		// Chrome 63+, Edge 18+, Opera 50+, Safari 11.1+, Firefox 58+, iOS 11+
		//
		// eslint-disable-next-line es-x/no-promise, es-x/no-promise-prototype-finally, dot-notation
		typeof Promise === 'function' && Promise.prototype[ 'finally' ] &&
		// ES6 Arrow Functions (with default params), this ensures
		// genuine syntax support for ES6 grammar, not just API coverage.
		//
		// https://caniuse.com/arrow-functions
		// Chrome 45+, Safari 10+, Firefox 22+, Opera 32+
		//
		// Based on Benjamin De Cock's snippet here:
		// https://gist.github.com/bendc/d7f3dbc83d0f65ca0433caf90378cd95
		( function () {
			try {
				// eslint-disable-next-line no-new, no-new-func
				new Function( '(a = 0) => a' );
				return true;
			} catch ( e ) {
				return false;
			}
		}() ) &&
		// ES6 RegExp.prototype.flags
		//
		// https://caniuse.com/mdn-javascript_builtins_regexp_flags
		// Edge 79+ (Chromium-based, rejects MSEdgeHTML-based Edge <= 18)
		//
		// eslint-disable-next-line es-x/no-regexp-prototype-flags
		/./g.flags === 'g'
	);
}

if ( !isCompatible() ) {
	// Handle basic supported browsers (Grade C).
	// Undo speculative modern (Grade A) root CSS class `<html class="client-js">`.
	// See ResourceLoaderClientHtml::getDocumentAttributes().
	document.documentElement.className = document.documentElement.className
		.replace( /(^|\s)client-js(\s|$)/, '$1client-nojs$2' );

	// Process any callbacks for basic support (Grade C).
	while ( window.NORLQ && NORLQ[ 0 ] ) {
		NORLQ.shift()();
	}
	NORLQ = {
		push: function ( fn ) {
			fn();
		}
	};

	// Clear and disable the modern (Grade A) queue.
	RLQ = {
		push: function () {}
	};
} else {
	// Handle modern (Grade A).

	if ( window.performance && performance.mark ) {
		performance.mark( 'mwStartup' );
	}

	// This embeds mediawiki.js, which defines 'mw' and 'mw.loader'.
	/**
 * Base library for MediaWiki.
 */
/* global $CODE */

( function () {
	'use strict';

	var con = window.console;

	/**
	 * Log a message to window.console.
	 *
	 * Useful to force logging of some errors that are otherwise hard to detect (i.e., this logs
	 * also in production mode).
	 *
	 * @private
	 * @param {string} topic Stream name passed by mw.track
	 * @param {Object} data Data passed by mw.track
	 * @param {Error} [data.exception]
	 * @param {string} data.source Error source
	 * @param {string} [data.module] Name of module which caused the error
	 */
	function logError( topic, data ) {
		var e = data.exception;
		var msg = ( e ? 'Exception' : 'Error' ) +
			' in ' + data.source +
			( data.module ? ' in module ' + data.module : '' ) +
			( e ? ':' : '.' );

		con.log( msg );

		// If we have an exception object, log it to the warning channel to trigger
		// proper stacktraces in browsers that support it.
		if ( e ) {
			con.warn( e );
		}
	}

	/**
	 * ES3 compatible class similar to [ES6 Map]{@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map}.
	 *
	 * @class mw.Map
	 * @classdesc Create an object that can be read from or written to via methods that allow
	 * interaction both with single and multiple properties at once.
	 * @example
	 *
	 *   const map = new mw.Map();
	 *   map.set( 'foo', 5 );
	 *   alert( 5 === map.get( 'foo' ) );
	 */
	function Map() {
		this.values = Object.create( null );
	}

	Map.prototype = {
		constructor: Map,

		/**
		 * Get the value of one or more keys.
		 *
		 * If called with no arguments, all values are returned.
		 *
		 * @memberof mw.Map
		 * @param {string|Array} [selection] Key or array of keys to retrieve values for.
		 * @param {any} [fallback=null] Value for keys that don't exist.
		 * @return {any|Object|null} If selection was a string, returns the value,
		 *  If selection was an array, returns an object of key/values.
		 *  If no selection is passed, a new object with all key/values is returned.
		 */
		get: function ( selection, fallback ) {
			if ( arguments.length < 2 ) {
				fallback = null;
			}

			if ( typeof selection === 'string' ) {
				return selection in this.values ?
					this.values[ selection ] :
					fallback;
			}

			var results;
			if ( Array.isArray( selection ) ) {
				results = {};
				for ( var i = 0; i < selection.length; i++ ) {
					if ( typeof selection[ i ] === 'string' ) {
						results[ selection[ i ] ] = selection[ i ] in this.values ?
							this.values[ selection[ i ] ] :
							fallback;
					}
				}
				return results;
			}

			if ( selection === undefined ) {
				results = {};
				for ( var key in this.values ) {
					results[ key ] = this.values[ key ];
				}
				return results;
			}

			// Invalid selection key
			return fallback;
		},

		/**
		 * Set one or more key/value pairs.
		 *
		 * @memberof mw.Map
		 * @param {string|Object} selection Key to set value for, or object mapping keys to values
		 * @param {Mixed} [value] Value to set (optional, only in use when key is a string)
		 * @return {boolean} True on success, false on failure
		 */
		set: function ( selection, value ) {
			// Use `arguments.length` because `undefined` is also a valid value.
			if ( arguments.length > 1 ) {
				// Set one key
				if ( typeof selection === 'string' ) {
					this.values[ selection ] = value;
					return true;
				}
			} else if ( typeof selection === 'object' ) {
				// Set multiple keys
				for ( var key in selection ) {
					this.values[ key ] = selection[ key ];
				}
				return true;
			}
			return false;
		},

		/**
		 * Check if a given key exists in the map.
		 *
		 * @memberof mw.Map
		 * @param {string} selection Key to check
		 * @return {boolean} True if the key exists
		 */
		exists: function ( selection ) {
			return typeof selection === 'string' && selection in this.values;
		}
	};

	/**
	 * Write a verbose message to the browser's console in debug mode.
	 *
	 * This method is mainly intended for verbose logging. It is a no-op in production mode.
	 * In ResourceLoader debug mode, it will use the browser's console.
	 *
	 * @ignore
	 * @param {...string} msg Messages to output to console.
	 */
	var log = function () {
		console.log.apply( console, arguments );
	};

	/**
	 * Write a message to the browser console's warning channel.
	 *
	 * @memberof mw.log
	 * @method warn
	 * @param {...string} msg Messages to output to console
	 */
	log.warn = Function.prototype.bind.call( con.warn, con );

	/**
	 * Base library for MediaWiki. Exposed globally as `mw`, with `mediaWiki` as alias.
	 *
	 * @namespace mw
	 * @singleton
	 * @hideconstructor
	 * @static
	 */
	var mw = {
		/**
		 * Get the current time, measured in milliseconds since January 1, 1970 (UTC).
		 *
		 * On browsers that implement the Navigation Timing API, this function will produce
		 * floating-point values with microsecond precision that are guaranteed to be monotonic.
		 * On all other browsers, it will fall back to using `Date`.
		 *
		 * @memberof mw
		 * @return {number} Current time
		 */
		now: function () {
			// Optimisation: Cache and re-use the chosen implementation.
			// Optimisation: Avoid startup overhead by re-defining on first call instead of IIFE.
			var perf = window.performance;
			var navStart = perf && perf.timing && perf.timing.navigationStart;

			// Define the relevant shortcut
			mw.now = navStart && perf.now ?
				function () {
					return navStart + perf.now();
				} :
				Date.now;

			return mw.now();
		},

		/**
		 * List of all analytic events emitted so far.
		 *
		 * Exposed only for use by mediawiki.base.
		 *
		 * @private
		 * @property {Array}
		 */
		trackQueue: [],

		/**
		 * Re-implements the mw.track method in
		 * resources/src/mediawiki.base/mediawiki.base.js. Thus ignored
		 * from public documentation.
		 *
		 * @ignore
		 * @param {any} topic that is being tracked
		 * @param {any} data data that is passed to the callback
		 */
		track: function ( topic, data ) {
			mw.trackQueue.push( { topic: topic, data: data } );
			// This method is extended by mediawiki.base to also fire events.
		},

		/**
		 * Track an early error event via mw.track and send it to the window console.
		 *
		 * @private
		 * @param {string} topic Topic name
		 * @param {Object} data Data describing the event, encoded as an object; see mw#logError
		 */
		trackError: function ( topic, data ) {
			mw.track( topic, data );
			logError( topic, data );
		},

		/**
		 * Library that predates the ES6 Map class with similar functionality.
		 *
		 * @type {mw.Map}
		 */
		Map: Map,

		/**
		 * Map of configuration values.
		 *
		 * Check out [the complete list of configuration values](https://www.mediawiki.org/wiki/Manual:Interface/JavaScript#mw.config)
		 * on mediawiki.org.
		 *
		 * @memberof mw
		 * @type {mw.Map}
		 */
		config: new Map(),

		/**
		 * Store for messages.
		 *
		 * @memberof mw
		 * @type {mw.Map}
		 */
		messages: new Map(),

		/**
		 * Store for templates associated with a module.
		 *
		 * @type {mw.Map}
		 * @memberof mw
		 */
		templates: new Map(),

		// Expose mw.log
		log: log

		// mw.loader is defined in a separate file that is appended to this
	};

	// Attach to window and globally alias
	window.mw = window.mediaWiki = mw;
}() );
/*!
 * Defines mw.loader, the infrastructure for loading ResourceLoader
 * modules.
 *
 * This file is appended directly to the code in startup/mediawiki.js
 */
/* global $VARS, $CODE, mw */

( function () {
	'use strict';

	var store,
		hasOwn = Object.hasOwnProperty;

	/**
	 * Client for ResourceLoader server end point.
	 *
	 * This client is in charge of maintaining the module registry and state
	 * machine, initiating network (batch) requests for loading modules, as
	 * well as dependency resolution and execution of source code.
	 *
	 * @see <https://www.mediawiki.org/wiki/ResourceLoader/Features>
	 * @namespace mw.loader
	 * @memberof mw
	 * @singleton
	 * @hideconstructor
	 * @static
	 */

	/**
	 * FNV132 hash function
	 *
	 * This function implements the 32-bit version of FNV-1.
	 * It is equivalent to hash( 'fnv132', ... ) in PHP, except
	 * its output is base 36 rather than hex.
	 * See <https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function>
	 *
	 * @private
	 * @param {string} str String to hash
	 * @return {string} hash as a five-character base 36 string
	 */
	function fnv132( str ) {
		var hash = 0x811C9DC5;

		/* eslint-disable no-bitwise */
		for ( var i = 0; i < str.length; i++ ) {
			hash += ( hash << 1 ) + ( hash << 4 ) + ( hash << 7 ) + ( hash << 8 ) + ( hash << 24 );
			hash ^= str.charCodeAt( i );
		}

		hash = ( hash >>> 0 ).toString( 36 ).slice( 0, 5 );
		/* eslint-enable no-bitwise */

		while ( hash.length < 5 ) {
			hash = '0' + hash;
		}
		return hash;
	}

	/**
	 * Fired via mw.track on various resource loading errors.
	 *
	 * eslint-disable jsdoc/valid-types
	 * @event ~'resourceloader.exception'
	 * @ignore
	 * @param {Error|Mixed} e The error that was thrown. Almost always an Error
	 *   object, but in theory module code could manually throw something else, and that
	 *   might also end up here.
	 * @param {string} [module] Name of the module which caused the error. Omitted if the
	 *   error is not module-related or the module cannot be easily identified due to
	 *   batched handling.
	 * @param {string} source Source of the error. Possible values:
	 *
	 *   - load-callback: exception thrown by user callback
	 *   - module-execute: exception thrown by module code
	 *   - resolve: failed to sort dependencies for a module in mw.loader.load
	 *   - store-eval: could not evaluate module code cached in localStorage
	 *   - store-localstorage-json: JSON conversion error in mw.loader.store
	 *   - store-localstorage-update: localStorage conversion error in mw.loader.store.
	 */

	/**
	 * Mapping of registered modules.
	 *
	 * See #implement and #execute for exact details on support for script, style and messages.
	 *
	 *     @example Format:
	 *
	 *     {
	 *         'moduleName': {
	 *             // From mw.loader.register()
	 *             'version': '#####' (five-character hash)
	 *             'dependencies': ['required.foo', 'bar.also', ...]
	 *             'group': string, integer, (or) null
	 *             'source': 'local', (or) 'anotherwiki'
	 *             'skip': 'return !!window.Example;', (or) null, (or) boolean result of skip
	 *             'module': export Object
	 *
	 *             // Set by execute() or mw.loader.state()
	 *             // See mw.loader.getState() for documentation of the state machine
	 *             'state': 'registered', 'loading', 'loaded', 'executing', 'ready', 'error', or 'missing'
	 *
	 *             // Optionally added at run-time by mw.loader.impl()
	 *             'script': closure, array of urls, or string
	 *             'style': { ... } (see #execute)
	 *             'messages': { 'key': 'value', ... }
	 *         }
	 *     }
	 *
	 * @property {Object}
	 * @private
	 */
	var registry = Object.create( null ),
		// Mapping of sources, keyed by source-id, values are strings.
		//
		// Format:
		//
		//     {
		//         'sourceId': 'http://example.org/w/load.php'
		//     }
		//
		sources = Object.create( null ),

		// For queueModuleScript()
		handlingPendingRequests = false,
		pendingRequests = [],

		// List of modules to be loaded
		queue = [],

		/**
		 * List of callback jobs waiting for modules to be ready.
		 *
		 * Jobs are created by #enqueue() and run by #doPropagation().
		 * Typically when a job is created for a module, the job's dependencies contain
		 * both the required module and all its recursive dependencies.
		 *
		 *     @example Format:
		 *
		 *     {
		 *         'dependencies': [ module names ],
		 *         'ready': Function callback
		 *         'error': Function callback
		 *     }
		 *
		 * @property {Object[]} jobs
		 * @private
		 */
		jobs = [],

		// For #setAndPropagate() and #doPropagation()
		willPropagate = false,
		errorModules = [],

		/**
		 * @private
		 * @property {Array} baseModules
		 */
		baseModules = [
    "jquery",
    "mediawiki.base"
],

		/**
		 * For #addEmbeddedCSS() and #addLink()
		 *
		 * @private
		 * @property {HTMLElement|null} marker
		 */
		marker = document.querySelector( 'meta[name="ResourceLoaderDynamicStyles"]' ),

		// For #addEmbeddedCSS()
		lastCssBuffer;

	/**
	 * Append an HTML element to `document.head` or before a specified node.
	 *
	 * @private
	 * @param {HTMLElement} el
	 * @param {Node|null} [nextNode]
	 */
	function addToHead( el, nextNode ) {
		if ( nextNode && nextNode.parentNode ) {
			nextNode.parentNode.insertBefore( el, nextNode );
		} else {
			document.head.appendChild( el );
		}
	}

	/**
	 * Create a new style element and add it to the DOM.
	 *
	 * @private
	 * @param {string} text CSS text
	 * @param {Node|null} [nextNode] The element where the style tag
	 *  should be inserted before
	 * @return {HTMLStyleElement} Reference to the created style element
	 */
	function newStyleTag( text, nextNode ) {
		var el = document.createElement( 'style' );
		el.appendChild( document.createTextNode( text ) );
		addToHead( el, nextNode );
		return el;
	}

	/**
	 * @private
	 * @param {Object} cssBuffer
	 */
	function flushCssBuffer( cssBuffer ) {
		// Make sure the next call to addEmbeddedCSS() starts a new buffer.
		// This must be done before we run the callbacks, as those may end up
		// queueing new chunks which would be lost otherwise (T105973).
		//
		// There can be more than one buffer in-flight (given "@import", and
		// generally due to race conditions). Only tell addEmbeddedCSS() to
		// start a new buffer if we're currently flushing the last one that it
		// started. If we're flushing an older buffer, keep the last one open.
		if ( cssBuffer === lastCssBuffer ) {
			lastCssBuffer = null;
		}
		newStyleTag( cssBuffer.cssText, marker );
		for ( var i = 0; i < cssBuffer.callbacks.length; i++ ) {
			cssBuffer.callbacks[ i ]();
		}
	}

	/**
	 * Add a bit of CSS text to the current browser page.
	 *
	 * The creation and insertion of the `<style>` element is debounced for two reasons:
	 *
	 * - Performing the insertion before the next paint round via requestAnimationFrame
	 *   avoids forced or wasted style recomputations, which are expensive in browsers.
	 * - Reduce how often new stylesheets are inserted by letting additional calls to this
	 *   function accumulate into a buffer for at least one JavaScript tick. Modules are
	 *   received from the server in batches, which means there is likely going to be many
	 *   calls to this function in a row within the same tick / the same call stack.
	 *   See also T47810.
	 *
	 * @private
	 * @param {string} cssText CSS text to be added in a `<style>` tag.
	 * @param {Function} callback Called after the insertion has occurred.
	 */
	function addEmbeddedCSS( cssText, callback ) {
		// Start a new buffer if one of the following is true:
		// - We've never started a buffer before, this will be our first.
		// - The last buffer we created was flushed meanwhile, so start a new one.
		// - The next CSS chunk syntactically needs to be at the start of a stylesheet (T37562).
		if ( !lastCssBuffer || cssText.startsWith( '@import' ) ) {
			lastCssBuffer = {
				cssText: '',
				callbacks: []
			};
			requestAnimationFrame( flushCssBuffer.bind( null, lastCssBuffer ) );
		}

		// Linebreak for somewhat distinguishable sections
		lastCssBuffer.cssText += '\n' + cssText;
		lastCssBuffer.callbacks.push( callback );
	}

	/**
	 * See also `ResourceLoader.php#makeVersionQuery` on the server.
	 *
	 * @private
	 * @param {string[]} modules List of module names
	 * @return {string} Hash of concatenated version hashes.
	 */
	function getCombinedVersion( modules ) {
		var hashes = modules.reduce( function ( result, module ) {
			return result + registry[ module ].version;
		}, '' );
		return fnv132( hashes );
	}

	/**
	 * Determine whether all dependencies are in state 'ready', which means we may
	 * execute the module or job now.
	 *
	 * @private
	 * @param {string[]} modules Names of modules to be checked
	 * @return {boolean} True if all modules are in state 'ready', false otherwise
	 */
	function allReady( modules ) {
		for ( var i = 0; i < modules.length; i++ ) {
			if ( mw.loader.getState( modules[ i ] ) !== 'ready' ) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Determine whether all direct and base dependencies are in state 'ready'
	 *
	 * @private
	 * @param {string} module Name of the module to be checked
	 * @return {boolean} True if all direct/base dependencies are in state 'ready'; false otherwise
	 */
	function allWithImplicitReady( module ) {
		return allReady( registry[ module ].dependencies ) &&
			( baseModules.indexOf( module ) !== -1 || allReady( baseModules ) );
	}

	/**
	 * Determine whether all dependencies are in state 'ready', which means we may
	 * execute the module or job now.
	 *
	 * @private
	 * @param {string[]} modules Names of modules to be checked
	 * @return {boolean|string} False if no modules are in state 'error' or 'missing';
	 *  failed module otherwise
	 */
	function anyFailed( modules ) {
		for ( var i = 0; i < modules.length; i++ ) {
			var state = mw.loader.getState( modules[ i ] );
			if ( state === 'error' || state === 'missing' ) {
				return modules[ i ];
			}
		}
		return false;
	}

	/**
	 * Handle propagation of module state changes and reactions to them.
	 *
	 * - When a module reaches a failure state, this should be propagated to
	 *   modules that depend on the failed module.
	 * - When a module reaches a final state, pending job callbacks for the
	 *   module from mw.loader.using() should be called.
	 * - When a module reaches the 'ready' state from #execute(), consider
	 *   executing dependent modules now having their dependencies satisfied.
	 * - When a module reaches the 'loaded' state from mw.loader.impl,
	 *   consider executing it, if it has no unsatisfied dependencies.
	 *
	 * @private
	 */
	function doPropagation() {
		var didPropagate = true;
		var module;

		// Keep going until the last iteration performed no actions.
		while ( didPropagate ) {
			didPropagate = false;

			// Stage 1: Propagate failures
			while ( errorModules.length ) {
				var errorModule = errorModules.shift(),
					baseModuleError = baseModules.indexOf( errorModule ) !== -1;
				for ( module in registry ) {
					if ( registry[ module ].state !== 'error' && registry[ module ].state !== 'missing' ) {
						if ( baseModuleError && baseModules.indexOf( module ) === -1 ) {
							// Propate error from base module to all regular (non-base) modules
							registry[ module ].state = 'error';
							didPropagate = true;
						} else if ( registry[ module ].dependencies.indexOf( errorModule ) !== -1 ) {
							// Propagate error from dependency to depending module
							registry[ module ].state = 'error';
							// .. and propagate it further
							errorModules.push( module );
							didPropagate = true;
						}
					}
				}
			}

			// Stage 2: Execute 'loaded' modules with no unsatisfied dependencies
			for ( module in registry ) {
				if ( registry[ module ].state === 'loaded' && allWithImplicitReady( module ) ) {
					// Recursively execute all dependent modules that were already loaded
					// (waiting for execution) and no longer have unsatisfied dependencies.
					// Base modules may have dependencies amongst eachother to ensure correct
					// execution order. Regular modules wait for all base modules.
					execute( module );
					didPropagate = true;
				}
			}

			// Stage 3: Invoke job callbacks that are no longer blocked
			for ( var i = 0; i < jobs.length; i++ ) {
				var job = jobs[ i ];
				var failed = anyFailed( job.dependencies );
				if ( failed !== false || allReady( job.dependencies ) ) {
					jobs.splice( i, 1 );
					i -= 1;
					try {
						if ( failed !== false && job.error ) {
							job.error( new Error( 'Failed dependency: ' + failed ), job.dependencies );
						} else if ( failed === false && job.ready ) {
							job.ready();
						}
					} catch ( e ) {
						// A user-defined callback raised an exception.
						// Swallow it to protect our state machine!
						mw.trackError( 'resourceloader.exception', {
							exception: e,
							source: 'load-callback'
						} );
					}
					didPropagate = true;
				}
			}
		}

		willPropagate = false;
	}

	/**
	 * Update a module's state in the registry and make sure any necessary
	 * propagation will occur, by adding a (debounced) call to doPropagation().
	 * See #doPropagation for more about propagation.
	 * See #registry for more about how states are used.
	 *
	 * @private
	 * @param {string} module
	 * @param {string} state
	 */
	function setAndPropagate( module, state ) {
		registry[ module ].state = state;
		if ( state === 'ready' ) {
			// Queue to later be synced to the local module store.
			store.add( module );
		} else if ( state === 'error' || state === 'missing' ) {
			errorModules.push( module );
		} else if ( state !== 'loaded' ) {
			// We only have something to do in doPropagation for the
			// 'loaded', 'ready', 'error', and 'missing' states.
			// Avoid scheduling and propagation cost for frequent and short-lived
			// transition states, such as 'loading' and 'executing'.
			return;
		}
		if ( willPropagate ) {
			// Already scheduled, or, we're already in a doPropagation stack.
			return;
		}
		willPropagate = true;
		// Yield for two reasons:
		// * Allow successive calls to mw.loader.impl() from the same
		//   load.php response, or from the same asyncEval() to be in the
		//   propagation batch.
		// * Allow the browser to breathe between the reception of
		//   module source code and the execution of it.
		//
		// Use a high priority because the user may be waiting for interactions
		// to start being possible. But, first provide a moment (up to 'timeout')
		// for native input event handling (e.g. scrolling/typing/clicking).
		mw.requestIdleCallback( doPropagation, { timeout: 1 } );
	}

	/**
	 * Resolve dependencies and detect circular references.
	 *
	 * @private
	 * @param {string} module Name of the top-level module whose dependencies shall be
	 *  resolved and sorted.
	 * @param {Array} resolved Returns a topological sort of the given module and its
	 *  dependencies, such that later modules depend on earlier modules. The array
	 *  contains the module names. If the array contains already some module names,
	 *  this function appends its result to the pre-existing array.
	 * @param {Set} [unresolved] Used to detect loops in the dependency graph.
	 * @throws {Error} If an unknown module or a circular dependency is encountered
	 */
	function sortDependencies( module, resolved, unresolved ) {
		if ( !( module in registry ) ) {
			throw new Error( 'Unknown module: ' + module );
		}

		if ( typeof registry[ module ].skip === 'string' ) {
			// eslint-disable-next-line no-new-func
			var skip = ( new Function( registry[ module ].skip )() );
			registry[ module ].skip = !!skip;
			if ( skip ) {
				registry[ module ].dependencies = [];
				setAndPropagate( module, 'ready' );
				return;
			}
		}

		// Create unresolved if not passed in
		if ( !unresolved ) {
			unresolved = new Set();
		}

		// Track down dependencies
		var deps = registry[ module ].dependencies;
		unresolved.add( module );
		for ( var i = 0; i < deps.length; i++ ) {
			if ( resolved.indexOf( deps[ i ] ) === -1 ) {
				if ( unresolved.has( deps[ i ] ) ) {
					throw new Error(
						'Circular reference detected: ' + module + ' -> ' + deps[ i ]
					);
				}

				sortDependencies( deps[ i ], resolved, unresolved );
			}
		}

		resolved.push( module );
	}

	/**
	 * Get names of module that a module depends on, in their proper dependency order.
	 *
	 * @private
	 * @param {string[]} modules Array of string module names
	 * @return {Array} List of dependencies, including 'module'.
	 * @throws {Error} If an unregistered module or a dependency loop is encountered
	 */
	function resolve( modules ) {
		// Always load base modules
		var resolved = baseModules.slice();
		for ( var i = 0; i < modules.length; i++ ) {
			sortDependencies( modules[ i ], resolved );
		}
		return resolved;
	}

	/**
	 * Like #resolve(), except it will silently ignore modules that
	 * are missing or have missing dependencies.
	 *
	 * @private
	 * @param {string[]} modules Array of string module names
	 * @return {Array} List of dependencies.
	 */
	function resolveStubbornly( modules ) {
		// Always load base modules
		var resolved = baseModules.slice();
		for ( var i = 0; i < modules.length; i++ ) {
			var saved = resolved.slice();
			try {
				sortDependencies( modules[ i ], resolved );
			} catch ( err ) {
				resolved = saved;
				// This module is not currently known, or has invalid dependencies.
				//
				// Most likely due to a cached reference after the module was
				// removed, otherwise made redundant, or omitted from the registry
				// by the ResourceLoader "target" system.
				//
				// These errors can be common, e.g. queuing an unavailable module
				// unconditionally from the server-side is OK and should fail gracefully.
				mw.log.warn( 'Skipped unavailable module ' + modules[ i ] );

				// Do not track this error as an exception when the module:
				// - Is valid, but gracefully filtered out by target system.
				// - Was recently valid, but is still referenced in stale cache.
				//
				// Basically the only reason to track this as exception is when the error
				// was circular or invalid dependencies. What the above scenarios have in
				// common is that they don't register the module client-side.
				if ( modules[ i ] in registry ) {
					mw.trackError( 'resourceloader.exception', {
						exception: err,
						source: 'resolve'
					} );
				}
			}
		}
		return resolved;
	}

	/**
	 * Resolve a relative file path.
	 *
	 * For example, resolveRelativePath( '../foo.js', 'resources/src/bar/bar.js' )
	 * returns 'resources/src/foo.js'.
	 *
	 * @private
	 * @param {string} relativePath Relative file path, starting with ./ or ../
	 * @param {string} basePath Path of the file (not directory) relativePath is relative to
	 * @return {string|null} Resolved path, or null if relativePath does not start with ./ or ../
	 */
	function resolveRelativePath( relativePath, basePath ) {
		// eslint-disable-next-line security/detect-unsafe-regex
		var relParts = relativePath.match( /^((?:\.\.?\/)+)(.*)$/ );
		if ( !relParts ) {
			return null;
		}

		var baseDirParts = basePath.split( '/' );
		// basePath looks like 'foo/bar/baz.js', so baseDirParts looks like [ 'foo', 'bar, 'baz.js' ]
		// Remove the file component at the end, so that we are left with only the directory path
		baseDirParts.pop();

		var prefixes = relParts[ 1 ].split( '/' );
		// relParts[ 1 ] looks like '../../', so prefixes looks like [ '..', '..', '' ]
		// Remove the empty element at the end
		prefixes.pop();

		// For every ../ in the path prefix, remove one directory level from baseDirParts
		var prefix;
		while ( ( prefix = prefixes.pop() ) !== undefined ) {
			if ( prefix === '..' ) {
				baseDirParts.pop();
			}
		}

		// If there's anything left of the base path, prepend it to the file path
		return ( baseDirParts.length ? baseDirParts.join( '/' ) + '/' : '' ) + relParts[ 2 ];
	}

	/**
	 * Make a require() function scoped to a package file
	 *
	 * @private
	 * @param {Object} moduleObj Module object from the registry
	 * @param {string} basePath Path of the file this is scoped to. Used for relative paths.
	 * @return {Function}
	 */
	function makeRequireFunction( moduleObj, basePath ) {
		return function require( moduleName ) {
			var fileName = resolveRelativePath( moduleName, basePath );
			if ( fileName === null ) {
				// Not a relative path, so it's either a module name or,
				// (if in test mode) a private file imported from another module.
				return mw.loader.require( moduleName );
			}

			if ( hasOwn.call( moduleObj.packageExports, fileName ) ) {
				// File has already been executed, return the cached result
				return moduleObj.packageExports[ fileName ];
			}

			var scriptFiles = moduleObj.script.files;
			if ( !hasOwn.call( scriptFiles, fileName ) ) {
				throw new Error( 'Cannot require undefined file ' + fileName );
			}

			var result,
				fileContent = scriptFiles[ fileName ];
			if ( typeof fileContent === 'function' ) {
				var moduleParam = { exports: {} };
				fileContent( makeRequireFunction( moduleObj, fileName ), moduleParam, moduleParam.exports );
				result = moduleParam.exports;
			} else {
				// fileContent is raw data (such as a JSON object), just pass it through
				result = fileContent;
			}
			moduleObj.packageExports[ fileName ] = result;
			return result;
		};
	}

	/**
	 * Load and execute a script.
	 *
	 * @private
	 * @param {string} src URL to script, will be used as the src attribute in the script tag
	 * @param {Function} [callback] Callback to run after request resolution
	 * @param {string[]} [modules] List of modules being requested, for state to be marked as error
	 * in case the script fails to load
	 * @return {HTMLElement}
	 */
	function addScript( src, callback, modules ) {
		// Use a <script> element rather than XHR. Using XHR changes the request
		// headers (potentially missing a cache hit), and reduces caching in general
		// since browsers cache XHR much less (if at all). And XHR means we retrieve
		// text, so we'd need to eval, which then messes up line numbers.
		// The drawback is that <script> does not offer progress events, feedback is
		// only given after downloading, parsing, and execution have completed.
		var script = document.createElement( 'script' );
		script.src = src;
		function onComplete() {
			if ( script.parentNode ) {
				script.parentNode.removeChild( script );
			}
			if ( callback ) {
				callback();
				callback = null;
			}
		}
		script.onload = onComplete;
		script.onerror = function () {
			onComplete();
			if ( modules ) {
				for ( var i = 0; i < modules.length; i++ ) {
					setAndPropagate( modules[ i ], 'error' );
				}
			}
		};
		document.head.appendChild( script );
		return script;
	}

	/**
	 * Queue the loading and execution of a script for a particular module.
	 *
	 * This does for legacy debug mode what runScript() does for production.
	 *
	 * @private
	 * @param {string} src URL of the script
	 * @param {string} moduleName Name of currently executing module
	 * @param {Function} callback Callback to run after addScript() resolution
	 */
	function queueModuleScript( src, moduleName, callback ) {
		pendingRequests.push( function () {
			// Keep in sync with execute()/runScript().
			if ( moduleName !== 'jquery' ) {
				window.require = mw.loader.require;
				window.module = registry[ moduleName ].module;
			}
			addScript( src, function () {
				// 'module.exports' should not persist after the file is executed to
				// avoid leakage to unrelated code. 'require' should be kept, however,
				// as asynchronous access to 'require' is allowed and expected. (T144879)
				delete window.module;
				callback();
				// Start the next one (if any)
				if ( pendingRequests[ 0 ] ) {
					pendingRequests.shift()();
				} else {
					handlingPendingRequests = false;
				}
			} );
		} );
		if ( !handlingPendingRequests && pendingRequests[ 0 ] ) {
			handlingPendingRequests = true;
			pendingRequests.shift()();
		}
	}

	/**
	 * Utility function for execute()
	 *
	 * @ignore
	 * @param {string} url URL
	 * @param {string} [media] Media attribute
	 * @param {Node|null} [nextNode]
	 * @return {HTMLElement}
	 */
	function addLink( url, media, nextNode ) {
		var el = document.createElement( 'link' );

		el.rel = 'stylesheet';
		if ( media ) {
			el.media = media;
		}
		// If you end up here from an IE exception "SCRIPT: Invalid property value.",
		// see #addEmbeddedCSS, T33676, T43331, and T49277 for details.
		el.href = url;

		addToHead( el, nextNode );
		return el;
	}

	/**
	 * Evaluate in the global scope.
	 *
	 * This is used by MediaWiki user scripts, where it is (for example)
	 * important that `var` makes a global variable.
	 *
	 * @private
	 * @param {string} code JavaScript code
	 */
	function globalEval( code ) {
		var script = document.createElement( 'script' );
		script.text = code;
		document.head.appendChild( script );
		script.parentNode.removeChild( script );
	}

	/**
	 * Evaluate JS code using indirect eval().
	 *
	 * This is used by mw.loader.store. It is important that we protect the
	 * integrity of mw.loader's private variables (from accidental clashes
	 * or re-assignment), which means we can't use regular `eval()`.
	 *
	 * Optimization: This exists separately from globalEval(), because that
	 * involves slow DOM overhead.
	 *
	 * @private
	 * @param {string} code JavaScript code
	 */
	function indirectEval( code ) {
		// See http://perfectionkills.com/global-eval-what-are-the-options/
		// for an explanation of this syntax.
		// eslint-disable-next-line no-eval
		( 1, eval )( code );
	}

	/**
	 * Add one or more modules to the module load queue.
	 *
	 * See also #work().
	 *
	 * @private
	 * @param {string[]} dependencies Array of module names in the registry
	 * @param {Function} [ready] Callback to execute when all dependencies are ready
	 * @param {Function} [error] Callback to execute when any dependency fails
	 */
	function enqueue( dependencies, ready, error ) {
		if ( allReady( dependencies ) ) {
			// Run ready immediately
			if ( ready ) {
				ready();
			}
			return;
		}

		var failed = anyFailed( dependencies );
		if ( failed !== false ) {
			if ( error ) {
				// Execute error immediately if any dependencies have errors
				error(
					new Error( 'Dependency ' + failed + ' failed to load' ),
					dependencies
				);
			}
			return;
		}

		// Not all dependencies are ready, add to the load queue...

		// Add ready and error callbacks if they were given
		if ( ready || error ) {
			jobs.push( {
				// Narrow down the list to modules that are worth waiting for
				dependencies: dependencies.filter( function ( module ) {
					var state = registry[ module ].state;
					return state === 'registered' || state === 'loaded' || state === 'loading' || state === 'executing';
				} ),
				ready: ready,
				error: error
			} );
		}

		dependencies.forEach( function ( module ) {
			// Only queue modules that are still in the initial 'registered' state
			// (e.g. not ones already loading or loaded etc.).
			if ( registry[ module ].state === 'registered' && queue.indexOf( module ) === -1 ) {
				queue.push( module );
			}
		} );

		mw.loader.work();
	}

	/**
	 * Executes a loaded module, making it ready to use
	 *
	 * @private
	 * @param {string} module Module name to execute
	 */
	function execute( module ) {
		if ( registry[ module ].state !== 'loaded' ) {
			throw new Error( 'Module in state "' + registry[ module ].state + '" may not execute: ' + module );
		}

		registry[ module ].state = 'executing';
		

		var runScript = function () {
			
			var script = registry[ module ].script;
			var markModuleReady = function () {
				
				setAndPropagate( module, 'ready' );
			};
			var nestedAddScript = function ( arr, offset ) {
				// Recursively call queueModuleScript() in its own callback
				// for each element of arr.
				if ( offset >= arr.length ) {
					// We're at the end of the array
					markModuleReady();
					return;
				}

				queueModuleScript( arr[ offset ], module, function () {
					nestedAddScript( arr, offset + 1 );
				} );
			};

			try {
				if ( Array.isArray( script ) ) {
					nestedAddScript( script, 0 );
				} else if ( typeof script === 'function' ) {
					// Keep in sync with queueModuleScript() for debug mode
					if ( module === 'jquery' ) {
						// This is a special case for when 'jquery' itself is being loaded.
						// - The standard jquery.js distribution does not set `window.jQuery`
						//   in CommonJS-compatible environments (Node.js, AMD, RequireJS, etc.).
						// - MediaWiki's 'jquery' module also bundles jquery.migrate.js, which
						//   in a CommonJS-compatible environment, will use require('jquery'),
						//   but that can't work when we're still inside that module.
						script();
					} else {
						// Pass jQuery twice so that the signature of the closure which wraps
						// the script can bind both '$' and 'jQuery'.
						script( window.$, window.$, mw.loader.require, registry[ module ].module );
					}
					markModuleReady();
				} else if ( typeof script === 'object' && script !== null ) {
					var mainScript = script.files[ script.main ];
					if ( typeof mainScript !== 'function' ) {
						throw new Error( 'Main file in module ' + module + ' must be a function' );
					}
					// jQuery parameters are not passed for multi-file modules
					mainScript(
						makeRequireFunction( registry[ module ], script.main ),
						registry[ module ].module,
						registry[ module ].module.exports
					);
					markModuleReady();
				} else if ( typeof script === 'string' ) {
					// Site and user modules are legacy scripts that run in the global scope.
					// This is transported as a string instead of a function to avoid needing
					// to use string manipulation to undo the function wrapper.
					globalEval( script );
					markModuleReady();

				} else {
					// Module without script
					markModuleReady();
				}
			} catch ( e ) {
				// Use mw.track instead of mw.log because these errors are common in production mode
				// (e.g. undefined variable), and mw.log is only enabled in debug mode.
				setAndPropagate( module, 'error' );
				
				mw.trackError( 'resourceloader.exception', {
					exception: e,
					module: module,
					source: 'module-execute'
				} );
			}
		};

		// Emit deprecation warnings
		if ( registry[ module ].deprecationWarning ) {
			mw.log.warn( registry[ module ].deprecationWarning );
		}

		// Add localizations to message system
		if ( registry[ module ].messages ) {
			mw.messages.set( registry[ module ].messages );
		}

		// Initialise templates
		if ( registry[ module ].templates ) {
			mw.templates.set( module, registry[ module ].templates );
		}

		// Adding of stylesheets is asynchronous via addEmbeddedCSS().
		// The below function uses a counting semaphore to make sure we don't call
		// runScript() until after this module's stylesheets have been inserted
		// into the DOM.
		var cssPending = 0;
		var cssHandle = function () {
			// Increase semaphore, when creating a callback for addEmbeddedCSS.
			cssPending++;
			return function () {
				// Decrease semaphore, when said callback is invoked.
				cssPending--;
				if ( cssPending === 0 ) {
					// Paranoia:
					// This callback is exposed to addEmbeddedCSS, which is outside the execute()
					// function and is not concerned with state-machine integrity. In turn,
					// addEmbeddedCSS() actually exposes stuff further via requestAnimationFrame.
					// If increment and decrement callbacks happen in the wrong order, or start
					// again afterwards, then this branch could be reached multiple times.
					// To protect the integrity of the state-machine, prevent that from happening
					// by making runScript() cannot be called more than once.  We store a private
					// reference when we first reach this branch, then deference the original, and
					// call our reference to it.
					var runScriptCopy = runScript;
					runScript = undefined;
					runScriptCopy();
				}
			};
		};

		// Process styles (see also mw.loader.impl)
		// * { "css": [css, ..] }
		// * { "url": { <media>: [url, ..] } }
		var style = registry[ module ].style;
		if ( style ) {
			// Array of CSS strings under key 'css'
			// { "css": [css, ..] }
			if ( 'css' in style ) {
				for ( var i = 0; i < style.css.length; i++ ) {
					addEmbeddedCSS( style.css[ i ], cssHandle() );
				}
			}

			// Plain object with array of urls under a media-type key
			// { "url": { <media>: [url, ..] } }
			if ( 'url' in style ) {
				for ( var media in style.url ) {
					var urls = style.url[ media ];
					for ( var j = 0; j < urls.length; j++ ) {
						addLink( urls[ j ], media, marker );
					}
				}
			}
		}

		// End profiling of execute()-self before we call runScript(),
		// which we want to measure separately without overlap.
		

		if ( module === 'user' ) {
			// Implicit dependency on the site module. Not a real dependency because it should
			// run after 'site' regardless of whether it succeeds or fails.
			// Note: This is a simplified version of mw.loader.using(), inlined here because
			// mw.loader.using() is part of mediawiki.base (depends on jQuery; T192623).
			var siteDeps;
			var siteDepErr;
			try {
				siteDeps = resolve( [ 'site' ] );
			} catch ( e ) {
				siteDepErr = e;
				runScript();
			}
			if ( !siteDepErr ) {
				enqueue( siteDeps, runScript, runScript );
			}
		} else if ( cssPending === 0 ) {
			// Regular module without styles
			runScript();
		}
		// else: runScript will get called via cssHandle()
	}

	function sortQuery( o ) {
		var sorted = {};
		var list = [];

		for ( var key in o ) {
			list.push( key );
		}
		list.sort();
		for ( var i = 0; i < list.length; i++ ) {
			sorted[ list[ i ] ] = o[ list[ i ] ];
		}
		return sorted;
	}

	/**
	 * Converts a module map of the form `{ foo: [ 'bar', 'baz' ], bar: [ 'baz, 'quux' ] }`
	 * to a query string of the form `foo.bar,baz|bar.baz,quux`.
	 *
	 * See `ResourceLoader::makePackedModulesString()` in PHP, of which this is a port.
	 * On the server, unpacking is done by `ResourceLoader::expandModuleNames()`.
	 *
	 * Note: This is only half of the logic, the other half has to be in #batchRequest(),
	 * because its implementation needs to keep track of potential string size in order
	 * to decide when to split the requests due to url size.
	 *
	 * @typedef {Object} ModuleString
	 * @property {string} str Module query string
	 * @property {Array} list List of module names in matching order
	 *
	 * @private
	 * @param {Object} moduleMap Module map
	 * @return {ModuleString}
	 */
	function buildModulesString( moduleMap ) {
		var str = [];
		var list = [];
		var p;

		function restore( suffix ) {
			return p + suffix;
		}

		for ( var prefix in moduleMap ) {
			p = prefix === '' ? '' : prefix + '.';
			str.push( p + moduleMap[ prefix ].join( ',' ) );
			list.push.apply( list, moduleMap[ prefix ].map( restore ) );
		}
		return {
			str: str.join( '|' ),
			list: list
		};
	}

	/**
	 * @private
	 * @param {Object} params Map of parameter names to values
	 * @return {string}
	 */
	function makeQueryString( params ) {
		// Optimisation: This is a fairly hot code path with batchRequest() loops.
		// Avoid overhead from Object.keys and Array.forEach.
		// String concatenation is faster than array pushing and joining, see
		// https://phabricator.wikimedia.org/P19931
		var str = '';
		for ( var key in params ) {
			// Parameters are separated by &, added before all parameters other than
			// the first
			str += ( str ? '&' : '' ) + encodeURIComponent( key ) + '=' +
				encodeURIComponent( params[ key ] );
		}
		return str;
	}

	/**
	 * Create network requests for a batch of modules.
	 *
	 * This is an internal method for #work(). This must not be called directly
	 * unless the modules are already registered, and no request is in progress,
	 * and the module state has already been set to `loading`.
	 *
	 * @private
	 * @param {string[]} batch
	 */
	function batchRequest( batch ) {
		if ( !batch.length ) {
			return;
		}

		var sourceLoadScript, currReqBase, moduleMap;

		/**
		 * Start the currently drafted request to the server.
		 *
		 * @ignore
		 */
		function doRequest() {
			// Optimisation: Inherit (Object.create), not copy ($.extend)
			var query = Object.create( currReqBase ),
				packed = buildModulesString( moduleMap );
			query.modules = packed.str;
			// The packing logic can change the effective order, even if the input was
			// sorted. As such, the call to getCombinedVersion() must use this
			// effective order to ensure that the combined version will match the hash
			// expected by the server based on combining versions from the module
			// query string in-order. (T188076)
			query.version = getCombinedVersion( packed.list );
			query = sortQuery( query );
			addScript( sourceLoadScript + '?' + makeQueryString( query ), null, packed.list );
		}

		// Always order modules alphabetically to help reduce cache
		// misses for otherwise identical content.
		batch.sort();

		// Query parameters common to all requests
		var reqBase = {
    "lang": "en",
    "skin": "vector",
    "debug": "1"
};

		// Split module list by source and by group.
		var splits = Object.create( null );
		for ( var b = 0; b < batch.length; b++ ) {
			var bSource = registry[ batch[ b ] ].source;
			var bGroup = registry[ batch[ b ] ].group;
			if ( !splits[ bSource ] ) {
				splits[ bSource ] = Object.create( null );
			}
			if ( !splits[ bSource ][ bGroup ] ) {
				splits[ bSource ][ bGroup ] = [];
			}
			splits[ bSource ][ bGroup ].push( batch[ b ] );
		}

		for ( var source in splits ) {
			sourceLoadScript = sources[ source ];

			for ( var group in splits[ source ] ) {

				// Cache access to currently selected list of
				// modules for this group from this source.
				var modules = splits[ source ][ group ];

				// Query parameters common to requests for this module group
				// Optimisation: Inherit (Object.create), not copy ($.extend)
				currReqBase = Object.create( reqBase );
				// User modules require a user name in the query string.
				if ( group === 0 && mw.config.get( 'wgUserName' ) !== null ) {
					currReqBase.user = mw.config.get( 'wgUserName' );
				}

				// In addition to currReqBase, doRequest() will also add 'modules' and 'version'.
				// > '&modules='.length === 9
				// > '&version=12345'.length === 14
				// > 9 + 14 = 23
				var currReqBaseLength = makeQueryString( currReqBase ).length + 23;

				// We may need to split up the request to honor the query string length limit,
				// so build it piece by piece. `length` does not include the characters from
				// the request base, see below
				var length = 0;
				moduleMap = Object.create( null ); // { prefix: [ suffixes ] }

				for ( var i = 0; i < modules.length; i++ ) {
					// Determine how many bytes this module would add to the query string
					var lastDotIndex = modules[ i ].lastIndexOf( '.' ),
						prefix = modules[ i ].slice( 0, Math.max( 0, lastDotIndex ) ),
						suffix = modules[ i ].slice( lastDotIndex + 1 ),
						bytesAdded = moduleMap[ prefix ] ?
							suffix.length + 3 : // '%2C'.length == 3
							modules[ i ].length + 3; // '%7C'.length == 3

					// If the url would become too long, create a new one, but don't create empty requests.
					// The value of `length` only reflects the request-specific bytes relating to the
					// accumulated entries in moduleMap so far. It does not include the base length,
					// which we account for separately with `currReqBaseLength` so that length is 0
					// when moduleMap is empty.
					if ( length && length + currReqBaseLength + bytesAdded > mw.loader.maxQueryLength ) {
						// Dispatch what we've got...
						doRequest();
						// .. and start preparing a new request.
						length = 0;
						moduleMap = Object.create( null );
					}
					if ( !moduleMap[ prefix ] ) {
						moduleMap[ prefix ] = [];
					}
					length += bytesAdded;
					moduleMap[ prefix ].push( suffix );
				}
				// Optimization: Skip `length` check.
				// moduleMap will contain at least one module here. The loop above leaves the last module
				// undispatched (and maybe some before it), so for moduleMap to be empty here, there must
				// have been no modules to iterate in the current group to start with, but we only create
				// a group in `splits` when the first module in the group is seen, so there are always
				// modules in the group when this code is reached.
				doRequest();
			}
		}
	}

	/**
	 * @private
	 * @param {string[]} implementations Array containing pieces of JavaScript code in the
	 *  form of calls to mw.loader#impl().
	 * @param {Function} cb Callback in case of failure
	 * @param {Error} cb.err
	 * @param {number} [offset] Integer offset into implementations to start at
	 */
	function asyncEval( implementations, cb, offset ) {
		if ( !implementations.length ) {
			return;
		}
		offset = offset || 0;
		mw.requestIdleCallback( function ( deadline ) {
			asyncEvalTask( deadline, implementations, cb, offset );
		} );
	}

	/**
	 * Idle callback for asyncEval
	 *
	 * @private
	 * @param {IdleDeadline} deadline
	 * @param {string[]} implementations
	 * @param {Function} cb
	 * @param {Error} cb.err
	 * @param {number} offset
	 */
	function asyncEvalTask( deadline, implementations, cb, offset ) {
		for ( var i = offset; i < implementations.length; i++ ) {
			if ( deadline.timeRemaining() <= 0 ) {
				asyncEval( implementations, cb, i );
				return;
			}
			try {
				indirectEval( implementations[ i ] );
			} catch ( err ) {
				cb( err );
			}
		}
	}

	/**
	 * Make a versioned key for a specific module.
	 *
	 * @private
	 * @param {string} module Module name
	 * @return {string|null} Module key in format '`[name]@[version]`',
	 *  or null if the module does not exist
	 */
	function getModuleKey( module ) {
		return module in registry ? ( module + '@' + registry[ module ].version ) : null;
	}

	/**
	 * @private
	 * @param {string} key Module name or '`[name]@[version]`'
	 * @return {Object}
	 */
	function splitModuleKey( key ) {
		// Module names may contain '@' but version strings may not, so the last '@' is the delimiter
		var index = key.lastIndexOf( '@' );
		// If the key doesn't contain '@' or starts with it, the whole thing is the module name
		if ( index === -1 || index === 0 ) {
			return {
				name: key,
				version: ''
			};
		}
		return {
			name: key.slice( 0, index ),
			version: key.slice( index + 1 )
		};
	}

	/**
	 * @private
	 * @param {string} module
	 * @param {string} [version]
	 * @param {string[]} [dependencies]
	 * @param {string} [group]
	 * @param {string} [source]
	 * @param {string} [skip]
	 */
	function registerOne( module, version, dependencies, group, source, skip ) {
		if ( module in registry ) {
			throw new Error( 'module already registered: ' + module );
		}

		registry[ module ] = {
			// Exposed to execute() for mw.loader.impl() closures.
			// Import happens via require().
			module: {
				exports: {}
			},
			// module.export objects for each package file inside this module
			packageExports: {},
			version: version || '',
			dependencies: dependencies || [],
			group: typeof group === 'undefined' ? null : group,
			source: typeof source === 'string' ? source : 'local',
			state: 'registered',
			skip: typeof skip === 'string' ? skip : null
		};
	}

	/* Public Members */

	mw.loader = {
		/**
		 * The module registry is exposed as an aid for debugging and inspecting page
		 * state; it is not a public interface for modifying the registry.
		 *
		 * @see #registry
		 * @property {Object}
		 * @private
		 */
		moduleRegistry: registry,

		/**
		 * Exposed for testing and debugging only.
		 *
		 * @see #batchRequest
		 * @property {number}
		 * @private
		 */
		maxQueryLength: 5000,

		addStyleTag: newStyleTag,

		// Exposed for internal use only. Documented as @private.
		addScriptTag: addScript,
		addLinkTag: addLink,

		enqueue: enqueue,

		resolve: resolve,

		/**
		 * Start loading of all queued module dependencies.
		 *
		 * @private
		 */
		work: function () {
			store.init();

			var q = queue.length,
				storedImplementations = [],
				storedNames = [],
				requestNames = [],
				batch = new Set();

			// Iterate the list of requested modules, and do one of three things:
			// - 1) Nothing (if already loaded or being loaded).
			// - 2) Eval the cached implementation from the module store.
			// - 3) Request from network.
			while ( q-- ) {
				var module = queue[ q ];
				// Only consider modules which are the initial 'registered' state,
				// and ignore duplicates
				if ( mw.loader.getState( module ) === 'registered' &&
					!batch.has( module )
				) {
					// Progress the state machine
					registry[ module ].state = 'loading';
					batch.add( module );

					var implementation = store.get( module );
					if ( implementation ) {
						// Module store enabled and contains this module/version
						storedImplementations.push( implementation );
						storedNames.push( module );
					} else {
						// Module store disabled or doesn't have this module/version
						requestNames.push( module );
					}
				}
			}

			// Now that the queue has been processed into a batch, clear the queue.
			// This MUST happen before we initiate any eval or network request. Otherwise,
			// it is possible for a cached script to instantly trigger the same work queue
			// again; all before we've cleared it causing each request to include modules
			// which are already loaded.
			queue = [];

			asyncEval( storedImplementations, function ( err ) {
				// Not good, the cached mw.loader.impl calls failed! This should
				// never happen, barring ResourceLoader bugs, browser bugs and PEBKACs.
				// Depending on how corrupt the string is, it is likely that some
				// modules' impl() succeeded while the ones after the error will
				// never run and leave their modules in the 'loading' state forever.
				store.stats.failed++;

				// Since this is an error not caused by an individual module but by
				// something that infected the implement call itself, don't take any
				// risks and clear everything in this cache.
				store.clear();

				mw.trackError( 'resourceloader.exception', {
					exception: err,
					source: 'store-eval'
				} );
				// For any failed ones, fallback to requesting from network
				var failed = storedNames.filter( function ( name ) {
					return registry[ name ].state === 'loading';
				} );
				batchRequest( failed );
			} );

			batchRequest( requestNames );
		},

		/**
		 * Register a source.
		 *
		 * The #work() method will use this information to split up requests by source.
		 *
		 *     @example
		 *     mw.loader.addSource( { mediawikiwiki: 'https://www.mediawiki.org/w/load.php' } );
		 *
		 * @private
		 * @param {Object} ids An object mapping ids to load.php end point urls
		 * @throws {Error} If source id is already registered
		 */
		addSource: function ( ids ) {
			for ( var id in ids ) {
				if ( id in sources ) {
					throw new Error( 'source already registered: ' + id );
				}
				sources[ id ] = ids[ id ];
			}
		},

		/**
		 * Register a module, letting the system know about it and its properties.
		 *
		 * The startup module calls this method.
		 *
		 * When using multiple module registration by passing an array, dependencies that
		 * are specified as references to modules within the array will be resolved before
		 * the modules are registered.
		 *
		 * @param {string|Array} modules Module name or array of arrays, each containing
		 *  a list of arguments compatible with this method
		 * @param {string} [version] Module version hash (falls backs to empty string)
		 * @param {string[]} [dependencies] Array of module names on which this module depends.
		 * @param {string} [group=null] Group which the module is in
		 * @param {string} [source='local'] Name of the source
		 * @param {string} [skip=null] Script body of the skip function
		 * @private
		 */
		register: function ( modules ) {
			if ( typeof modules !== 'object' ) {
				registerOne.apply( null, arguments );
				return;
			}
			// Need to resolve indexed dependencies:
			// ResourceLoader uses an optimisation to save space which replaces module
			// names in dependency lists with the index of that module within the
			// array of module registration data if it exists. The benefit is a significant
			// reduction in the data size of the startup module. This loop changes
			// those dependency lists back to arrays of strings.
			function resolveIndex( dep ) {
				return typeof dep === 'number' ? modules[ dep ][ 0 ] : dep;
			}

			for ( var i = 0; i < modules.length; i++ ) {
				var deps = modules[ i ][ 2 ];
				if ( deps ) {
					for ( var j = 0; j < deps.length; j++ ) {
						deps[ j ] = resolveIndex( deps[ j ] );
					}
				}
				// Optimisation: Up to 55% faster.
				// Typically register() is called exactly once on a page, and with a batch.
				// See <https://gist.github.com/Krinkle/f06fdb3de62824c6c16f02a0e6ce0e66>
				// Benchmarks taught us that the code for adding an object to `registry`
				// should be in a function that has only one signature and does no arguments
				// manipulation.
				// JS semantics make it hard to optimise recursion to a different
				// signature of itself, hence we moved this out.
				registerOne.apply( null, modules[ i ] );
			}
		},

		/**
		 * Implement a module given the components of the module.
		 *
		 * See #impl for a full description of the parameters.
		 *
		 * Prior to MW 1.41, this was used internally, but now it is only kept
		 * for backwards compatibility.
		 *
		 * Does not support mw.loader.store caching.
		 *
		 * @param {string} module
		 * @param {Function|Array|string|Object} [script]
		 * @param {Object} [style]
		 * @param {Object} [messages] List of key/value pairs to be added to mw#messages.
		 * @param {Object} [templates] List of key/value pairs to be added to mw#templates.
		 * @param {string|null} [deprecationWarning] Deprecation warning if any
		 * @private
		 */
		implement: function ( module, script, style, messages, templates, deprecationWarning ) {
			var split = splitModuleKey( module ),
				name = split.name,
				version = split.version;

			// Automatically register module
			if ( !( name in registry ) ) {
				mw.loader.register( name );
			}
			// Check for duplicate implementation
			if ( registry[ name ].script !== undefined ) {
				throw new Error( 'module already implemented: ' + name );
			}
			registry[ name ].version = version;
			registry[ name ].declarator = null; // not supported
			registry[ name ].script = script;
			registry[ name ].style = style;
			registry[ name ].messages = messages;
			registry[ name ].templates = templates;
			registry[ name ].deprecationWarning = deprecationWarning;
			// The module may already have been marked as erroneous
			if ( registry[ name ].state !== 'error' && registry[ name ].state !== 'missing' ) {
				setAndPropagate( name, 'loaded' );
			}
		},

		/**
		 * Implement a module given a function which returns the components of the module
		 *
		 * @param {Function} declarator
		 *
		 * The declarator should return an array with the following keys:
		 *
		 *  - 0. {string} module Name of module and current module version. Formatted
		 *    as '`[name]@[version]`". This version should match the requested version
		 *    (from #batchRequest and #registry). This avoids race conditions (T117587).
		 *
		 *  - 1. {Function|Array|string|Object} [script] Module code. This can be a function,
		 *    a list of URLs to load via `<script src>`, a string for `globalEval()`, or an
		 *    object like {"files": {"foo.js":function, "bar.js": function, ...}, "main": "foo.js"}.
		 *    If an object is provided, the main file will be executed immediately, and the other
		 *    files will only be executed if loaded via require(). If a function or string is
		 *    provided, it will be executed/evaluated immediately. If an array is provided, all
		 *    URLs in the array will be loaded immediately, and executed as soon as they arrive.
		 *
		 *  - 2. {Object} [style] Should follow one of the following patterns:
		 *
		 *     { "css": [css, ..] }
		 *     { "url": { (media): [url, ..] } }
		 *
		 *    The reason css strings are not concatenated anymore is T33676. We now check
		 *    whether it's safe to extend the stylesheet.
		 *
		 *  - 3. {Object} [messages] List of key/value pairs to be added to mw#messages.
		 *  - 4. {Object} [templates] List of key/value pairs to be added to mw#templates.
		 *  - 5. {String|null} [deprecationWarning] Deprecation warning if any
		 *
		 * The declarator must not use any scope variables, since it will be serialized with
		 * Function.prototype.toString() and later restored and executed in the global scope.
		 *
		 * The elements are all optional except the name.
		 * @private
		 */
		impl: function ( declarator ) {
			var data = declarator(),
				module = data[ 0 ],
				script = data[ 1 ] || null,
				style = data[ 2 ] || null,
				messages = data[ 3 ] || null,
				templates = data[ 4 ] || null,
				deprecationWarning = data[ 5 ] || null,
				split = splitModuleKey( module ),
				name = split.name,
				version = split.version;

			// Automatically register module
			if ( !( name in registry ) ) {
				mw.loader.register( name );
			}
			// Check for duplicate implementation
			if ( registry[ name ].script !== undefined ) {
				throw new Error( 'module already implemented: ' + name );
			}
			// Without this reset, if there is a version mismatch between the
			// requested and received module version, then mw.loader.store would
			// cache the response under the requested key. Thus poisoning the cache
			// indefinitely with a stale value. (T117587)
			registry[ name ].version = version;
			// Attach components
			registry[ name ].declarator = declarator;
			registry[ name ].script = script;
			registry[ name ].style = style;
			registry[ name ].messages = messages;
			registry[ name ].templates = templates;
			registry[ name ].deprecationWarning = deprecationWarning;
			// The module may already have been marked as erroneous
			if ( registry[ name ].state !== 'error' && registry[ name ].state !== 'missing' ) {
				setAndPropagate( name, 'loaded' );
			}
		},

		/**
		 * Load an external script or one or more modules.
		 *
		 * This method takes a list of unrelated modules. Use cases:
		 *
		 * - A web page will be composed of many different widgets. These widgets independently
		 *   queue their ResourceLoader modules (`OutputPage::addModules()`). If any of them
		 *   have problems, or are no longer known (e.g. cached HTML), the other modules
		 *   should still be loaded.
		 * - This method is used for preloading, which must not throw. Later code that
		 *   calls #using() will handle the error.
		 *
		 * @param {string|Array} modules Either the name of a module, array of modules,
		 *  or a URL of an external script or style
		 * @param {string} [type='text/javascript'] MIME type to use if calling with a URL of an
		 *  external script or style; acceptable values are "text/css" and
		 *  "text/javascript"; if no type is provided, text/javascript is assumed.
		 * @throws {Error} If type is invalid
		 */
		load: function ( modules, type ) {
			// eslint-disable-next-line security/detect-unsafe-regex
			if ( typeof modules === 'string' && /^(https?:)?\/?\//.test( modules ) ) {
				// Called with a url like so:
				// - "https://example.org/x.js"
				// - "http://example.org/x.js"
				// - "//example.org/x.js"
				// - "/x.js"
				if ( type === 'text/css' ) {
					addLink( modules );
				} else if ( type === 'text/javascript' || type === undefined ) {
					addScript( modules );
				} else {
					// Unknown type
					throw new Error( 'Invalid type ' + type );
				}
			} else {
				// One or more modules
				modules = typeof modules === 'string' ? [ modules ] : modules;
				// Resolve modules into a flat list for internal queuing.
				// This also filters out unknown modules and modules with
				// unknown dependencies, allowing the rest to continue. (T36853)
				// Omit ready and error parameters, we don't have callbacks
				enqueue( resolveStubbornly( modules ) );
			}
		},

		/**
		 * Change the state of one or more modules.
		 *
		 * @param {Object} states Object of module name/state pairs
		 * @private
		 */
		state: function ( states ) {
			for ( var module in states ) {
				if ( !( module in registry ) ) {
					mw.loader.register( module );
				}
				setAndPropagate( module, states[ module ] );
			}
		},

		/**
		 * Get the state of a module.
		 *
		 * Possible states for the public API:
		 *
		 * - `registered`: The module is available for loading but not yet requested.
		 * - `loading`, `loaded`, or `executing`: The module is currently being loaded.
		 * - `ready`: The module was succesfully and fully loaded.
		 * - `error`: The module or one its dependencies has failed to load, e.g. due to
		 *    uncaught error from the module's script files.
		 * - `missing`: The module was requested but is not defined according to the server.
		 *
		 * Internal mw.loader state machine:
		 *
		 * - `registered`:
		 *    The module is known to the system but not yet required.
		 *    Meta data is stored by mw.loader#register.
		 *    Calls to that method are generated server-side by StartupModule.
		 * - `loading`:
		 *    The module was required through mw.loader (either directly or as dependency of
		 *    another module). The client will fetch module contents from mw.loader.store
		 *    or from the server. The contents should later be received by mw.loader#implement.
		 * - `loaded`:
		 *    The module has been received by mw.loader#implement.
		 *    Once the module has no more dependencies in-flight, the module will be executed,
		 *    controlled via #setAndPropagate and #doPropagation.
		 * - `executing`:
		 *    The module is being executed (apply messages and stylesheets, execute scripts)
		 *    by mw.loader#execute.
		 * - `ready`:
		 *    The module has been successfully executed.
		 * - `error`:
		 *    The module (or one of its dependencies) produced an uncaught error during execution.
		 * - `missing`:
		 *    The module was registered client-side and requested, but the server denied knowledge
		 *    of the module's existence.
		 *
		 * @param {string} module Name of module
		 * @return {string|null} The state, or null if the module (or its state) is not
		 *  in the registry.
		 */
		getState: function ( module ) {
			return module in registry ? registry[ module ].state : null;
		},

		/**
		 * Get the exported value of a module.
		 *
		 * This static method is publicly exposed for debugging purposes
		 * only and must not be used in production code. In production code,
		 * please use the dynamically provided `require()` function instead.
		 *
		 * In case of lazy-loaded modules via mw.loader#using(), the returned
		 * Promise provides the function, see #using() for examples.
		 *
		 * @private
		 * @since 1.27
		 * @param {string} moduleName Module name
		 * @return {Mixed} Exported value
		 */
		require: function ( moduleName ) {
			var path;
			if ( window.QUnit ) {
				// Comply with Node specification
				// https://nodejs.org/docs/v20.1.0/api/modules.html#all-together
				//
				// > Interpret X as a combination of NAME and SUBPATH, where the NAME
				// > may have a "@scope/" prefix and the subpath begins with a slash (`/`).
				//
				// Regex inspired by Node [1], but simplified to suite our purposes
				// and split in two in order to keep the Regex Star Height under 2,
				// as per ESLint security/detect-unsafe-regex.
				//
				// These patterns match "@scope/module/dir/file.js" and "module/dir/file.js"
				// respectively. They must not match "module.name" or "@scope/module.name".
				//
				// [1] https://github.com/nodejs/node/blob/v20.1.0/lib/internal/modules/cjs/loader.js#L554-L560
				var paths = moduleName.startsWith( '@' ) ?
					/^(@[^/]+\/[^/]+)\/(.*)$/.exec( moduleName ) :
					// eslint-disable-next-line no-mixed-spaces-and-tabs
					        /^([^/]+)\/(.*)$/.exec( moduleName );
				if ( paths ) {
					moduleName = paths[ 1 ];
					path = paths[ 2 ];
				}
			}

			// Only ready modules can be required
			if ( mw.loader.getState( moduleName ) !== 'ready' ) {
				// Module may've forgotten to declare a dependency
				throw new Error( 'Module "' + moduleName + '" is not loaded' );
			}

			return path ?
				makeRequireFunction( registry[ moduleName ], '' )( './' + path ) :
				registry[ moduleName ].module.exports;
		}
	};

	var hasPendingFlush = false,
		hasPendingWrites = false;

	/**
	 * Actually update the store
	 *
	 * @see #requestUpdate
	 * @private
	 */
	function flushWrites() {
		// Process queued module names, serialise their contents to the in-memory store.
		while ( store.queue.length ) {
			store.set( store.queue.shift() );
		}

		// Optimization: Don't reserialize the entire store and rewrite localStorage,
		// if no module was added or changed.
		if ( hasPendingWrites ) {
			// Remove anything from the in-memory store that came from previous page
			// loads that no longer corresponds with current module names and versions.
			store.prune();

			try {
				// Replacing the content of the module store might fail if the new
				// contents would exceed the browser's localStorage size limit. To
				// avoid clogging the browser with stale data, always remove the old
				// value before attempting to store a new one.
				localStorage.removeItem( store.key );
				localStorage.setItem( store.key, JSON.stringify( {
					items: store.items,
					vary: store.vary,
					// Store with 1e7 ms accuracy (1e4 seconds, or ~ 2.7 hours),
					// which is enough for the purpose of expiring after ~ 30 days.
					asOf: Math.ceil( Date.now() / 1e7 )
				} ) );
			} catch ( e ) {
				mw.trackError( 'resourceloader.exception', {
					exception: e,
					source: 'store-localstorage-update'
				} );
			}
		}

		// Let the next call to requestUpdate() create a new timer.
		hasPendingFlush = hasPendingWrites = false;
	}

	// We use a local variable `store` so that its easier to access, but also need to set
	// this in mw.loader so its exported - combine the two

	/**
	 * On browsers that implement the localStorage API, the module store serves as a
	 * smart complement to the browser cache. Unlike the browser cache, the module store
	 * can slice a concatenated response from ResourceLoader into its constituent
	 * modules and cache each of them separately, using each module's versioning scheme
	 * to determine when the cache should be invalidated.
	 *
	 * @private
	 * @singleton
	 * @class mw.loader.store
	 * @ignore
	 */
	mw.loader.store = store = {
		// Whether the store is in use on this page.
		enabled: null,

		// The contents of the store, mapping '[name]@[version]' keys
		// to module implementations.
		items: {},

		// Names of modules to be stored during the next update.
		// See add() and update().
		queue: [],

		// Cache hit stats
		stats: { hits: 0, misses: 0, expired: 0, failed: 0 },

		/**
		 * The localStorage key for the entire module store. The key references
		 * $wgDBname to prevent clashes between wikis which share a common host.
		 *
		 * @property {string}
		 */
		key: "MediaWikiModuleStore:enwiki",

		/**
		 * A string containing various factors by which the module cache should vary.
		 *
		 * Defined by ResourceLoader\StartupModule::getStoreVary() in PHP.
		 *
		 * @property {string}
		 */
		vary: "vector:2:1:en",

		/**
		 * Initialize the store.
		 *
		 * Retrieves store from localStorage and (if successfully retrieved) decoding
		 * the stored JSON value to a plain object.
		 */
		init: function () {
			// Init only once per page
			if ( this.enabled === null ) {
				this.enabled = false;
				if ( false ) {
					this.load();
				} else {
					// Clear any previous store to free up space. (T66721)
					this.clear();
				}

			}
		},

		/**
		 * Internal helper for init(). Separated for ease of testing.
		 */
		load: function () {
			// These are the scenarios to think about:
			//
			// 1. localStorage is disallowed by the browser.
			//    This means `localStorage.getItem` throws.
			//    The store stays disabled.
			//
			// 2. localStorage did not contain our store key.
			//    This usually means the browser has a cold cache for this site,
			//    and thus localStorage.getItem returns null.
			//    The store will be enabled, and `items` starts fresh.
			//
			// 3. localStorage contains parseable data, but it's not usable.
			//    This means the data is too old, or is not valid for mw.loader.store.vary
			//    (e.g. user switched skin or language).
			//    The store will be enabled, and `items` starts fresh.
			//
			// 4. localStorage contains invalid JSON data.
			//    This means the data was corrupted, and `JSON.parse` throws.
			//    The store will be enabled, and `items` starts fresh.
			//
			// 5. localStorage contains valid and usable JSON.
			//    This means we have a warm cache from a previous visit.
			//    The store will be enabled, and `items` starts with the stored data.

			try {
				var raw = localStorage.getItem( this.key );

				// If we make it here, localStorage is enabled and available.
				// The rest of the function may fail, but that only affects what we load from
				// the cache. We'll still enable the store to allow storing new modules.
				this.enabled = true;

				// If getItem returns null, JSON.parse() will cast to string and re-parse, still null.
				var data = JSON.parse( raw );
				if ( data &&
					data.vary === this.vary &&
					data.items &&
					// Only use if it's been less than 30 days since the data was written
					// 30 days = 2,592,000 s = 2,592,000,000 ms = ± 259e7 ms
					Date.now() < ( data.asOf * 1e7 ) + 259e7
				) {
					// The data is not corrupt, matches our vary context, and has not expired.
					this.items = data.items;
				}
			} catch ( e ) {
				// Ignore error from localStorage or JSON.parse.
				// Don't print any warning (T195647).
			}
		},

		/**
		 * Retrieve a module from the store and update cache hit stats.
		 *
		 * @param {string} module Module name
		 * @return {string|boolean} Module implementation or false if unavailable
		 */
		get: function ( module ) {
			if ( this.enabled ) {
				var key = getModuleKey( module );
				if ( key in this.items ) {
					this.stats.hits++;
					return this.items[ key ];
				}

				this.stats.misses++;
			}

			return false;
		},

		/**
		 * Queue the name of a module that the next update should consider storing.
		 *
		 * @since 1.32
		 * @param {string} module Module name
		 */
		add: function ( module ) {
			if ( this.enabled ) {
				this.queue.push( module );
				this.requestUpdate();
			}
		},

		/**
		 * Add the contents of the named module to the in-memory store.
		 *
		 * This method does not guarantee that the module will be stored.
		 * Inspection of the module's meta data and size will ultimately decide that.
		 *
		 * This method is considered internal to mw.loader.store and must only
		 * be called if the store is enabled.
		 *
		 * @private
		 * @param {string} module Module name
		 */
		set: function ( module ) {
			var descriptor = registry[ module ],
				key = getModuleKey( module );

			if (
				// Already stored a copy of this exact version
				key in this.items ||
				// Module failed to load
				!descriptor ||
				descriptor.state !== 'ready' ||
				// Unversioned, private, or site-/user-specific
				!descriptor.version ||
				descriptor.group === 1 ||
				descriptor.group === 0 ||
				// Legacy descriptor, registered with mw.loader.implement
				!descriptor.declarator
			) {
				// Decline to store
				return;
			}

			var script = String( descriptor.declarator );
			// Modules whose serialised form exceeds 100 kB won't be stored (T66721).
			if ( script.length > 1e5 ) {
				return;
			}

			var srcParts = [
				'mw.loader.impl(',
				script,
				');\n'
			];
			if ( true ) {
				srcParts.push( '// Saved in localStorage at ', ( new Date() ).toISOString(), '\n' );
				var sourceLoadScript = sources[ descriptor.source ];
				var query = Object.create( {
    "lang": "en",
    "skin": "vector",
    "debug": "1"
} );
				query.modules = module;
				query.version = getCombinedVersion( [ module ] );
				query = sortQuery( query );
				srcParts.push(
					'//# sourceURL=',
					// Use absolute URL so that Firefox console stack trace links will work
					( new URL( sourceLoadScript, location ) ).href,
					'?',
					makeQueryString( query ),
					'\n'
				);

				query.sourcemap = '1';
				query = sortQuery( query );
				srcParts.push(
					'//# sourceMappingURL=',
					sourceLoadScript,
					'?',
					makeQueryString( query )
				);
			}
			this.items[ key ] = srcParts.join( '' );
			hasPendingWrites = true;
		},

		/**
		 * Iterate through the module store, removing any item that does not correspond
		 * (in name and version) to an item in the module registry.
		 */
		prune: function () {
			for ( var key in this.items ) {
				// key is in the form [name]@[version], slice to get just the name
				// to provide to getModuleKey, which will return a key in the same
				// form but with the latest version
				if ( getModuleKey( splitModuleKey( key ).name ) !== key ) {
					this.stats.expired++;
					delete this.items[ key ];
				}
			}
		},

		/**
		 * Clear the entire module store right now.
		 */
		clear: function () {
			this.items = {};
			try {
				localStorage.removeItem( this.key );
			} catch ( e ) {}
		},

		/**
		 * Request a sync of the in-memory store back to persisted localStorage.
		 *
		 * This function debounces updates. The debouncing logic should account
		 * for the following factors:
		 *
		 * - Writing to localStorage is an expensive operation that must not happen
		 *   during the critical path of initialising and executing module code.
		 *   Instead, it should happen at a later time after modules have been given
		 *   time and priority to do their thing first.
		 *
		 * - This method is called from mw.loader.store.add(), which will be called
		 *   hundreds of times on a typical page, including within the same call-stack
		 *   and eventloop-tick. This is because responses from load.php happen in
		 *   batches. As such, we want to allow all modules from the same load.php
		 *   response to be written to disk with a single flush, not many.
		 *
		 * - Repeatedly deleting and creating timers is non-trivial.
		 *
		 * - localStorage is shared by all pages from the same origin, if multiple
		 *   pages are loaded with different module sets, the possibility exists that
		 *   modules saved by one page will be clobbered by another. The impact of
		 *   this is minor, it merely causes a less efficient cache use, and the
		 *   problem would be corrected by subsequent page views.
		 *
		 * This method is considered internal to mw.loader.store and must only
		 * be called if the store is enabled.
		 *
		 * @private
		 * @method
		 */
		requestUpdate: function () {
			// On the first call to requestUpdate(), create a timer that
			// waits at least two seconds, then calls onTimeout.
			// The main purpose is to allow the current batch of load.php
			// responses to complete before we do anything. This batch can
			// trigger many hundreds of calls to requestUpdate().
			if ( !hasPendingFlush ) {
				hasPendingFlush = setTimeout(
					// Defer the actual write via requestIdleCallback
					function () {
						mw.requestIdleCallback( flushWrites );
					},
					2000
				);
			}
		}
	};

}() );
/* global mw */
mw.requestIdleCallbackInternal = function ( callback ) {
	setTimeout( function () {
		var start = mw.now();
		callback( {
			didTimeout: false,
			timeRemaining: function () {
				// Hard code a target maximum busy time of 50 milliseconds
				return Math.max( 0, 50 - ( mw.now() - start ) );
			}
		} );
	}, 1 );
};

/**
 * Schedule a deferred task to run in the background.
 *
 * This allows code to perform tasks in the main thread without impacting
 * time-critical operations such as animations and response to input events.
 *
 * Basic logic is as follows:
 *
 * - User input event should be acknowledged within 100ms per [RAIL][].
 * - Idle work should be grouped in blocks of upto 50ms so that enough time
 *   remains for the event handler to execute and any rendering to take place.
 * - Whenever a native event happens (e.g. user input), the deadline for any
 *   running idle callback drops to 0.
 * - As long as the deadline is non-zero, other callbacks pending may be
 *   executed in the same idle period.
 *
 * See also:
 *
 * - <https://developer.mozilla.org/en-US/docs/Web/API/Window/requestIdleCallback>
 * - <https://w3c.github.io/requestidlecallback/>
 * - <https://developers.google.com/web/updates/2015/08/using-requestidlecallback>
 *
 * [RAIL]: https://developers.google.com/web/fundamentals/performance/rail
 *
 * @memberof mw
 * @param {Function} callback
 * @param {Object} [options]
 * @param {number} [options.timeout] If set, the callback will be scheduled for
 *  immediate execution after this amount of time (in milliseconds) if it didn't run
 *  by that time.
 */
mw.requestIdleCallback = window.requestIdleCallback ?
	// Bind because it throws TypeError if context is not window
	window.requestIdleCallback.bind( window ) :
	mw.requestIdleCallbackInternal;
// Note: Polyfill was previously disabled due to
// https://bugs.chromium.org/p/chromium/issues/detail?id=647870
// See also <http://codepen.io/Krinkle/full/XNGEvv>


	/**
	 * The $CODE placeholder is substituted in ResourceLoaderStartUpModule.php.
	 */
	( function () {
		/* global mw */
		var queue;

		mw.loader.addSource({
    "local": "/w/load.php",
    "metawiki": "//meta.wikimedia.org/w/load.php"
});
mw.loader.register([
    [
        "site",
        "",
        [
            1
        ]
    ],
    [
        "site.styles",
        "",
        [],
        2
    ],
    [
        "filepage",
        ""
    ],
    [
        "user",
        "",
        [],
        0
    ],
    [
        "user.styles",
        "",
        [],
        0
    ],
    [
        "user.options",
        "",
        [],
        1
    ],
    [
        "mediawiki.skinning.interface",
        ""
    ],
    [
        "jquery.makeCollapsible.styles",
        ""
    ],
    [
        "mediawiki.skinning.content.parsoid",
        ""
    ],
    [
        "jquery",
        ""
    ],
    [
        "web2017-polyfills",
        "",
        [],
        null,
        null,
        "return 'IntersectionObserver' in window \u0026\u0026\n    typeof fetch === 'function' \u0026\u0026\n    // Ensure:\n    // - standards compliant URL\n    // - standards compliant URLSearchParams\n    // - URL#toJSON method (came later)\n    //\n    // Facts:\n    // - All browsers with URL also have URLSearchParams, don't need to check.\n    // - Safari \u003C= 7 and Chrome \u003C= 31 had a buggy URL implementations.\n    // - Firefox 29-43 had an incomplete URLSearchParams implementation. https://caniuse.com/urlsearchparams\n    // - URL#toJSON was released in Firefox 54, Safari 11, and Chrome 71. https://caniuse.com/mdn-api_url_tojson\n    //   Thus we don't need to check for buggy URL or incomplete URLSearchParams.\n    typeof URL === 'function' \u0026\u0026 'toJSON' in URL.prototype;\n"
    ],
    [
        "mediawiki.base",
        "",
        [
            9
        ]
    ],
    [
        "jquery.chosen",
        ""
    ],
    [
        "jquery.client",
        ""
    ],
    [
        "jquery.confirmable",
        "",
        [
            103
        ]
    ],
    [
        "jquery.highlightText",
        "",
        [
            77
        ]
    ],
    [
        "jquery.i18n",
        "",
        [
            102
        ]
    ],
    [
        "jquery.lengthLimit",
        "",
        [
            60
        ]
    ],
    [
        "jquery.makeCollapsible",
        "",
        [
            7,
            77
        ]
    ],
    [
        "jquery.spinner",
        "",
        [
            20
        ]
    ],
    [
        "jquery.spinner.styles",
        ""
    ],
    [
        "jquery.suggestions",
        "",
        [
            15
        ]
    ],
    [
        "jquery.tablesorter",
        "",
        [
            23,
            104,
            77
        ]
    ],
    [
        "jquery.tablesorter.styles",
        ""
    ],
    [
        "jquery.textSelection",
        "",
        [
            13
        ]
    ],
    [
        "jquery.ui",
        ""
    ],
    [
        "moment",
        "",
        [
            100,
            77
        ]
    ],
    [
        "vue",
        "",
        [
            111
        ]
    ],
    [
        "@vue/composition-api",
        "",
        [
            27
        ]
    ],
    [
        "vuex",
        "",
        [
            27
        ]
    ],
    [
        "pinia",
        "",
        [
            27
        ]
    ],
    [
        "@wikimedia/codex",
        "",
        [
            32,
            27
        ]
    ],
    [
        "codex-styles",
        ""
    ],
    [
        "@wikimedia/codex-search",
        "",
        [
            34,
            27
        ]
    ],
    [
        "codex-search-styles",
        ""
    ],
    [
        "mediawiki.template",
        ""
    ],
    [
        "mediawiki.template.mustache",
        "",
        [
            35
        ]
    ],
    [
        "mediawiki.apipretty",
        ""
    ],
    [
        "mediawiki.api",
        "",
        [
            67,
            103
        ]
    ],
    [
        "mediawiki.content.json",
        ""
    ],
    [
        "mediawiki.confirmCloseWindow",
        ""
    ],
    [
        "mediawiki.debug",
        "",
        [
            190
        ]
    ],
    [
        "mediawiki.diff",
        "",
        [
            189
        ]
    ],
    [
        "mediawiki.diff.styles",
        ""
    ],
    [
        "mediawiki.feedback",
        "",
        [
            882,
            198
        ]
    ],
    [
        "mediawiki.feedlink",
        ""
    ],
    [
        "mediawiki.filewarning",
        "",
        [
            190,
            202
        ]
    ],
    [
        "mediawiki.ForeignApi",
        "",
        [
            295
        ]
    ],
    [
        "mediawiki.ForeignApi.core",
        "",
        [
            74,
            38,
            187
        ]
    ],
    [
        "mediawiki.helplink",
        ""
    ],
    [
        "mediawiki.hlist",
        ""
    ],
    [
        "mediawiki.htmlform",
        "",
        [
            17,
            77
        ]
    ],
    [
        "mediawiki.htmlform.ooui",
        "",
        [
            190
        ]
    ],
    [
        "mediawiki.htmlform.styles",
        ""
    ],
    [
        "mediawiki.htmlform.ooui.styles",
        ""
    ],
    [
        "mediawiki.icon",
        ""
    ],
    [
        "mediawiki.inspect",
        "",
        [
            60,
            77
        ]
    ],
    [
        "mediawiki.notification",
        "",
        [
            77,
            83
        ]
    ],
    [
        "mediawiki.notification.convertmessagebox",
        "",
        [
            57
        ]
    ],
    [
        "mediawiki.notification.convertmessagebox.styles",
        ""
    ],
    [
        "mediawiki.String",
        ""
    ],
    [
        "mediawiki.pager.styles",
        ""
    ],
    [
        "mediawiki.pager.tablePager",
        ""
    ],
    [
        "mediawiki.pulsatingdot",
        ""
    ],
    [
        "mediawiki.searchSuggest",
        "",
        [
            21,
            38
        ]
    ],
    [
        "mediawiki.storage",
        "",
        [
            77
        ]
    ],
    [
        "mediawiki.toggleAllCollapsibles",
        "",
        [
            77
        ]
    ],
    [
        "mediawiki.Title",
        "",
        [
            60,
            77
        ]
    ],
    [
        "mediawiki.Upload",
        "",
        [
            38
        ]
    ],
    [
        "mediawiki.ForeignUpload",
        "",
        [
            47,
            68
        ]
    ],
    [
        "mediawiki.Upload.Dialog",
        "",
        [
            71
        ]
    ],
    [
        "mediawiki.Upload.BookletLayout",
        "",
        [
            68,
            75,
            26,
            193,
            198,
            203,
            204
        ]
    ],
    [
        "mediawiki.ForeignStructuredUpload.BookletLayout",
        "",
        [
            69,
            71,
            107,
            169,
            163
        ]
    ],
    [
        "mediawiki.toc",
        "",
        [
            80
        ]
    ],
    [
        "mediawiki.Uri",
        "",
        [
            77
        ]
    ],
    [
        "mediawiki.user",
        "",
        [
            38,
            80
        ]
    ],
    [
        "mediawiki.userSuggest",
        "",
        [
            21,
            38
        ]
    ],
    [
        "mediawiki.util",
        "",
        [
            13,
            10
        ]
    ],
    [
        "mediawiki.checkboxtoggle",
        ""
    ],
    [
        "mediawiki.checkboxtoggle.styles",
        ""
    ],
    [
        "mediawiki.cookie",
        ""
    ],
    [
        "mediawiki.experiments",
        ""
    ],
    [
        "mediawiki.editfont.styles",
        ""
    ],
    [
        "mediawiki.visibleTimeout",
        ""
    ],
    [
        "mediawiki.action.edit",
        "",
        [
            24,
            85,
            82,
            165
        ]
    ],
    [
        "mediawiki.action.edit.styles",
        ""
    ],
    [
        "mediawiki.action.edit.collapsibleFooter",
        "",
        [
            18,
            55,
            65
        ]
    ],
    [
        "mediawiki.action.edit.preview",
        "",
        [
            19,
            113
        ]
    ],
    [
        "mediawiki.action.history",
        "",
        [
            18
        ]
    ],
    [
        "mediawiki.action.history.styles",
        ""
    ],
    [
        "mediawiki.action.protect",
        "",
        [
            17,
            190
        ]
    ],
    [
        "mediawiki.action.view.metadata",
        "",
        [
            98
        ]
    ],
    [
        "mediawiki.editRecovery.postEdit",
        ""
    ],
    [
        "mediawiki.editRecovery.edit",
        "",
        [
            57,
            162,
            206
        ]
    ],
    [
        "mediawiki.action.view.postEdit",
        "",
        [
            57,
            65,
            153,
            190,
            209
        ]
    ],
    [
        "mediawiki.action.view.redirect",
        ""
    ],
    [
        "mediawiki.action.view.redirectPage",
        ""
    ],
    [
        "mediawiki.action.edit.editWarning",
        "",
        [
            24,
            40,
            103
        ]
    ],
    [
        "mediawiki.action.view.filepage",
        ""
    ],
    [
        "mediawiki.action.styles",
        ""
    ],
    [
        "mediawiki.language",
        "",
        [
            101
        ]
    ],
    [
        "mediawiki.cldr",
        "",
        [
            102
        ]
    ],
    [
        "mediawiki.libs.pluralruleparser",
        ""
    ],
    [
        "mediawiki.jqueryMsg",
        "",
        [
            60,
            100,
            77,
            5
        ]
    ],
    [
        "mediawiki.language.months",
        "",
        [
            100
        ]
    ],
    [
        "mediawiki.language.names",
        "",
        [
            100
        ]
    ],
    [
        "mediawiki.language.specialCharacters",
        "",
        [
            100
        ]
    ],
    [
        "mediawiki.libs.jpegmeta",
        ""
    ],
    [
        "mediawiki.page.gallery",
        "",
        [
            109,
            77
        ]
    ],
    [
        "mediawiki.page.gallery.styles",
        ""
    ],
    [
        "mediawiki.page.gallery.slideshow",
        "",
        [
            193,
            212,
            214
        ]
    ],
    [
        "mediawiki.page.ready",
        "",
        [
            38
        ]
    ],
    [
        "mediawiki.page.watch.ajax",
        "",
        [
            75
        ]
    ],
    [
        "mediawiki.page.preview",
        "",
        [
            18,
            24,
            42,
            43,
            75
        ]
    ],
    [
        "mediawiki.page.image.pagination",
        "",
        [
            19,
            77
        ]
    ],
    [
        "mediawiki.page.media",
        ""
    ],
    [
        "mediawiki.rcfilters.filters.base.styles",
        ""
    ],
    [
        "mediawiki.rcfilters.highlightCircles.seenunseen.styles",
        ""
    ],
    [
        "mediawiki.rcfilters.filters.ui",
        "",
        [
            18,
            74,
            75,
            160,
            199,
            206,
            208,
            209,
            210,
            212,
            213
        ]
    ],
    [
        "mediawiki.interface.helpers.styles",
        ""
    ],
    [
        "mediawiki.special",
        ""
    ],
    [
        "mediawiki.special.apisandbox",
        "",
        [
            18,
            74,
            180,
            166,
            189
        ]
    ],
    [
        "mediawiki.special.block",
        "",
        [
            51,
            163,
            179,
            170,
            180,
            177,
            206
        ]
    ],
    [
        "mediawiki.misc-authed-ooui",
        "",
        [
            19,
            52,
            160,
            165
        ]
    ],
    [
        "mediawiki.misc-authed-pref",
        "",
        [
            5
        ]
    ],
    [
        "mediawiki.misc-authed-curate",
        "",
        [
            12,
            14,
            17,
            19,
            38
        ]
    ],
    [
        "mediawiki.special.changeslist",
        ""
    ],
    [
        "mediawiki.special.changeslist.watchlistexpiry",
        "",
        [
            120,
            209
        ]
    ],
    [
        "mediawiki.special.changeslist.enhanced",
        ""
    ],
    [
        "mediawiki.special.changeslist.legend",
        ""
    ],
    [
        "mediawiki.special.changeslist.legend.js",
        "",
        [
            18,
            80
        ]
    ],
    [
        "mediawiki.special.contributions",
        "",
        [
            18,
            163,
            189
        ]
    ],
    [
        "mediawiki.special.import.styles.ooui",
        ""
    ],
    [
        "mediawiki.special.changecredentials",
        ""
    ],
    [
        "mediawiki.special.changeemail",
        ""
    ],
    [
        "mediawiki.special.preferences.ooui",
        "",
        [
            40,
            82,
            58,
            65,
            170,
            165,
            198
        ]
    ],
    [
        "mediawiki.special.preferences.styles.ooui",
        ""
    ],
    [
        "mediawiki.special.editrecovery.styles",
        ""
    ],
    [
        "mediawiki.special.editrecovery",
        "",
        [
            27
        ]
    ],
    [
        "mediawiki.special.search",
        "",
        [
            182
        ]
    ],
    [
        "mediawiki.special.search.commonsInterwikiWidget",
        "",
        [
            74,
            38
        ]
    ],
    [
        "mediawiki.special.search.interwikiwidget.styles",
        ""
    ],
    [
        "mediawiki.special.search.styles",
        ""
    ],
    [
        "mediawiki.special.unwatchedPages",
        "",
        [
            38
        ]
    ],
    [
        "mediawiki.special.upload",
        "",
        [
            19,
            38,
            40,
            107,
            120,
            35
        ]
    ],
    [
        "mediawiki.special.userlogin.common.styles",
        ""
    ],
    [
        "mediawiki.special.userlogin.login.styles",
        ""
    ],
    [
        "mediawiki.special.createaccount",
        "",
        [
            38
        ]
    ],
    [
        "mediawiki.special.userlogin.signup.styles",
        ""
    ],
    [
        "mediawiki.special.userrights",
        "",
        [
            17,
            58
        ]
    ],
    [
        "mediawiki.special.watchlist",
        "",
        [
            190,
            209
        ]
    ],
    [
        "mediawiki.tempUserBanner.styles",
        ""
    ],
    [
        "mediawiki.tempUserBanner",
        "",
        [
            103
        ]
    ],
    [
        "mediawiki.tempUserCreated",
        "",
        [
            77
        ]
    ],
    [
        "mediawiki.ui",
        ""
    ],
    [
        "mediawiki.ui.checkbox",
        ""
    ],
    [
        "mediawiki.ui.radio",
        ""
    ],
    [
        "mediawiki.ui.button",
        ""
    ],
    [
        "mediawiki.ui.input",
        ""
    ],
    [
        "mediawiki.ui.icon",
        ""
    ],
    [
        "mediawiki.widgets",
        "",
        [
            161,
            193,
            203,
            204
        ]
    ],
    [
        "mediawiki.widgets.styles",
        ""
    ],
    [
        "mediawiki.widgets.AbandonEditDialog",
        "",
        [
            198
        ]
    ],
    [
        "mediawiki.widgets.DateInputWidget",
        "",
        [
            164,
            26,
            193,
            214
        ]
    ],
    [
        "mediawiki.widgets.DateInputWidget.styles",
        ""
    ],
    [
        "mediawiki.widgets.visibleLengthLimit",
        "",
        [
            17,
            190
        ]
    ],
    [
        "mediawiki.widgets.datetime",
        "",
        [
            190,
            209,
            213,
            214
        ]
    ],
    [
        "mediawiki.widgets.expiry",
        "",
        [
            166,
            26,
            193
        ]
    ],
    [
        "mediawiki.widgets.CheckMatrixWidget",
        "",
        [
            190
        ]
    ],
    [
        "mediawiki.widgets.CategoryMultiselectWidget",
        "",
        [
            47,
            193
        ]
    ],
    [
        "mediawiki.widgets.SelectWithInputWidget",
        "",
        [
            171,
            193
        ]
    ],
    [
        "mediawiki.widgets.SelectWithInputWidget.styles",
        ""
    ],
    [
        "mediawiki.widgets.SizeFilterWidget",
        "",
        [
            173,
            193
        ]
    ],
    [
        "mediawiki.widgets.SizeFilterWidget.styles",
        ""
    ],
    [
        "mediawiki.widgets.MediaSearch",
        "",
        [
            47,
            75,
            193
        ]
    ],
    [
        "mediawiki.widgets.Table",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.widgets.TagMultiselectWidget",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.widgets.UserInputWidget",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.widgets.UsersMultiselectWidget",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.widgets.NamespacesMultiselectWidget",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.widgets.TitlesMultiselectWidget",
        "",
        [
            160
        ]
    ],
    [
        "mediawiki.widgets.TagMultiselectWidget.styles",
        ""
    ],
    [
        "mediawiki.widgets.SearchInputWidget",
        "",
        [
            64,
            160,
            209
        ]
    ],
    [
        "mediawiki.widgets.SearchInputWidget.styles",
        ""
    ],
    [
        "mediawiki.widgets.ToggleSwitchWidget",
        "",
        [
            193
        ]
    ],
    [
        "mediawiki.watchstar.widgets",
        "",
        [
            189
        ]
    ],
    [
        "mediawiki.deflate",
        ""
    ],
    [
        "oojs",
        ""
    ],
    [
        "mediawiki.router",
        "",
        [
            187
        ]
    ],
    [
        "oojs-ui",
        "",
        [
            196,
            193,
            198
        ]
    ],
    [
        "oojs-ui-core",
        "",
        [
            111,
            187,
            192,
            191,
            200
        ]
    ],
    [
        "oojs-ui-core.styles",
        ""
    ],
    [
        "oojs-ui-core.icons",
        ""
    ],
    [
        "oojs-ui-widgets",
        "",
        [
            190,
            195
        ]
    ],
    [
        "oojs-ui-widgets.styles",
        ""
    ],
    [
        "oojs-ui-widgets.icons",
        ""
    ],
    [
        "oojs-ui-toolbars",
        "",
        [
            190,
            197
        ]
    ],
    [
        "oojs-ui-toolbars.icons",
        ""
    ],
    [
        "oojs-ui-windows",
        "",
        [
            190,
            199
        ]
    ],
    [
        "oojs-ui-windows.icons",
        ""
    ],
    [
        "oojs-ui.styles.indicators",
        ""
    ],
    [
        "oojs-ui.styles.icons-accessibility",
        ""
    ],
    [
        "oojs-ui.styles.icons-alerts",
        ""
    ],
    [
        "oojs-ui.styles.icons-content",
        ""
    ],
    [
        "oojs-ui.styles.icons-editing-advanced",
        ""
    ],
    [
        "oojs-ui.styles.icons-editing-citation",
        ""
    ],
    [
        "oojs-ui.styles.icons-editing-core",
        ""
    ],
    [
        "oojs-ui.styles.icons-editing-list",
        ""
    ],
    [
        "oojs-ui.styles.icons-editing-styling",
        ""
    ],
    [
        "oojs-ui.styles.icons-interactions",
        ""
    ],
    [
        "oojs-ui.styles.icons-layout",
        ""
    ],
    [
        "oojs-ui.styles.icons-location",
        ""
    ],
    [
        "oojs-ui.styles.icons-media",
        ""
    ],
    [
        "oojs-ui.styles.icons-moderation",
        ""
    ],
    [
        "oojs-ui.styles.icons-movement",
        ""
    ],
    [
        "oojs-ui.styles.icons-user",
        ""
    ],
    [
        "oojs-ui.styles.icons-wikimedia",
        ""
    ],
    [
        "skins.vector.user",
        "",
        [],
        0
    ],
    [
        "skins.vector.user.styles",
        "",
        [],
        0
    ],
    [
        "skins.vector.search",
        "",
        [
            33,
            74
        ]
    ],
    [
        "skins.vector.styles.legacy",
        ""
    ],
    [
        "skins.vector.styles",
        ""
    ],
    [
        "skins.vector.icons.js",
        ""
    ],
    [
        "skins.vector.icons",
        ""
    ],
    [
        "skins.vector.clientPreferences",
        "",
        [
            75
        ]
    ],
    [
        "skins.vector.js",
        "",
        [
            81,
            112,
            65,
            224,
            222
        ]
    ],
    [
        "skins.vector.legacy.js",
        "",
        [
            111
        ]
    ],
    [
        "skins.monobook.styles",
        ""
    ],
    [
        "skins.monobook.scripts",
        "",
        [
            75,
            202
        ]
    ],
    [
        "skins.modern",
        ""
    ],
    [
        "skins.cologneblue",
        ""
    ],
    [
        "skins.timeless",
        ""
    ],
    [
        "skins.timeless.js",
        ""
    ],
    [
        "ext.timeline.styles",
        ""
    ],
    [
        "ext.wikihiero",
        ""
    ],
    [
        "ext.wikihiero.special",
        "",
        [
            234,
            19,
            190
        ]
    ],
    [
        "ext.wikihiero.visualEditor",
        "",
        [
            419
        ]
    ],
    [
        "ext.charinsert",
        "",
        [
            24
        ]
    ],
    [
        "ext.charinsert.styles",
        ""
    ],
    [
        "ext.cite.styles",
        ""
    ],
    [
        "ext.cite.parsoid.styles",
        ""
    ],
    [
        "ext.cite.visualEditor.core",
        "",
        [
            427
        ]
    ],
    [
        "ext.cite.visualEditor",
        "",
        [
            240,
            239,
            241,
            202,
            205,
            209
        ]
    ],
    [
        "ext.cite.wikiEditor",
        "",
        [
            343
        ]
    ],
    [
        "ext.cite.ux-enhancements",
        ""
    ],
    [
        "ext.citeThisPage",
        ""
    ],
    [
        "ext.inputBox.styles",
        ""
    ],
    [
        "ext.imagemap",
        "",
        [
            248
        ]
    ],
    [
        "ext.imagemap.styles",
        ""
    ],
    [
        "ext.pygments",
        ""
    ],
    [
        "ext.pygments.linenumbers",
        "",
        [
            77
        ]
    ],
    [
        "ext.geshi.visualEditor",
        "",
        [
            419
        ]
    ],
    [
        "ext.flaggedRevs.basic",
        ""
    ],
    [
        "ext.flaggedRevs.advanced",
        "",
        [
            19,
            42,
            43,
            119
        ]
    ],
    [
        "ext.flaggedRevs.review",
        "",
        [
            75
        ]
    ],
    [
        "ext.flaggedRevs.icons",
        ""
    ],
    [
        "ext.categoryTree",
        "",
        [
            38
        ]
    ],
    [
        "ext.categoryTree.styles",
        ""
    ],
    [
        "ext.spamBlacklist.visualEditor",
        ""
    ],
    [
        "mediawiki.api.titleblacklist",
        "",
        [
            38
        ]
    ],
    [
        "ext.titleblacklist.visualEditor",
        ""
    ],
    [
        "ext.tmh.video-js",
        ""
    ],
    [
        "ext.tmh.videojs-ogvjs",
        "",
        [
            270,
            261
        ]
    ],
    [
        "ext.tmh.player",
        "",
        [
            269,
            266,
            67
        ]
    ],
    [
        "ext.tmh.player.dialog",
        "",
        [
            265,
            198
        ]
    ],
    [
        "ext.tmh.player.inline",
        "",
        [
            269,
            261,
            67
        ]
    ],
    [
        "ext.tmh.player.styles",
        ""
    ],
    [
        "ext.tmh.transcodetable",
        "",
        [
            189
        ]
    ],
    [
        "ext.tmh.timedtextpage.styles",
        ""
    ],
    [
        "ext.tmh.OgvJsSupport",
        ""
    ],
    [
        "ext.tmh.OgvJs",
        "",
        [
            269
        ]
    ],
    [
        "embedPlayerIframeStyle",
        ""
    ],
    [
        "ext.urlShortener.special",
        "",
        [
            74,
            52,
            160,
            189
        ]
    ],
    [
        "ext.urlShortener.toolbar",
        ""
    ],
    [
        "ext.securepoll.htmlform",
        "",
        [
            19,
            177,
            189
        ]
    ],
    [
        "ext.securepoll",
        ""
    ],
    [
        "ext.securepoll.special",
        ""
    ],
    [
        "ext.score.visualEditor",
        "",
        [
            278,
            419
        ]
    ],
    [
        "ext.score.visualEditor.icons",
        ""
    ],
    [
        "ext.score.popup",
        "",
        [
            38
        ]
    ],
    [
        "ext.score.errors",
        ""
    ],
    [
        "ext.cirrus.serp",
        "",
        [
            74,
            188
        ]
    ],
    [
        "ext.nuke.confirm",
        "",
        [
            103
        ]
    ],
    [
        "ext.confirmEdit.editPreview.ipwhitelist.styles",
        ""
    ],
    [
        "ext.confirmEdit.visualEditor",
        "",
        [
            862
        ]
    ],
    [
        "ext.confirmEdit.simpleCaptcha",
        ""
    ],
    [
        "ext.confirmEdit.fancyCaptcha.styles",
        ""
    ],
    [
        "ext.confirmEdit.fancyCaptcha",
        "",
        [
            38
        ]
    ],
    [
        "ext.confirmEdit.fancyCaptchaMobile",
        "",
        [
            477
        ]
    ],
    [
        "ext.centralauth",
        "",
        [
            19,
            77
        ]
    ],
    [
        "ext.centralauth.centralautologin",
        "",
        [
            103
        ]
    ],
    [
        "ext.centralauth.centralautologin.clearcookie",
        ""
    ],
    [
        "ext.centralauth.misc.styles",
        ""
    ],
    [
        "ext.centralauth.globaluserautocomplete",
        "",
        [
            21,
            38
        ]
    ],
    [
        "ext.centralauth.globalrenameuser",
        "",
        [
            77
        ]
    ],
    [
        "ext.centralauth.ForeignApi",
        "",
        [
            48
        ]
    ],
    [
        "ext.widgets.GlobalUserInputWidget",
        "",
        [
            193
        ]
    ],
    [
        "ext.centralauth.globalrenamequeue",
        ""
    ],
    [
        "ext.centralauth.globalrenamequeue.styles",
        ""
    ],
    [
        "ext.GlobalUserPage",
        ""
    ],
    [
        "ext.apifeatureusage",
        ""
    ],
    [
        "ext.dismissableSiteNotice",
        "",
        [
            80,
            77
        ]
    ],
    [
        "ext.dismissableSiteNotice.styles",
        ""
    ],
    [
        "ext.centralNotice.startUp",
        "",
        [
            305
        ]
    ],
    [
        "ext.centralNotice.geoIP",
        "",
        [
            80
        ]
    ],
    [
        "ext.centralNotice.choiceData",
        "",
        [
            309
        ]
    ],
    [
        "ext.centralNotice.display",
        "",
        [
            304,
            307,
            571,
            74,
            65
        ]
    ],
    [
        "ext.centralNotice.kvStore",
        ""
    ],
    [
        "ext.centralNotice.bannerHistoryLogger",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.impressionDiet",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.largeBannerLimit",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.legacySupport",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.bannerSequence",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.freegeoipLookup",
        "",
        [
            304
        ]
    ],
    [
        "ext.centralNotice.impressionEventsSampleRate",
        "",
        [
            306
        ]
    ],
    [
        "ext.centralNotice.cspViolationAlert",
        ""
    ],
    [
        "ext.wikimediamessages.contactpage",
        ""
    ],
    [
        "mediawiki.special.block.feedback.request",
        ""
    ],
    [
        "ext.collection",
        "",
        [
            320,
            100
        ]
    ],
    [
        "ext.collection.bookcreator.styles",
        ""
    ],
    [
        "ext.collection.bookcreator",
        "",
        [
            319,
            65
        ]
    ],
    [
        "ext.collection.checkLoadFromLocalStorage",
        "",
        [
            318
        ]
    ],
    [
        "ext.collection.suggest",
        "",
        [
            320
        ]
    ],
    [
        "ext.collection.offline",
        ""
    ],
    [
        "ext.collection.bookcreator.messageBox",
        "",
        [
            325,
            50
        ]
    ],
    [
        "ext.collection.bookcreator.messageBox.icons",
        ""
    ],
    [
        "ext.ElectronPdfService.print.styles",
        ""
    ],
    [
        "ext.ElectronPdfService.special.styles",
        ""
    ],
    [
        "ext.ElectronPdfService.special.selectionImages",
        ""
    ],
    [
        "ext.advancedSearch.initialstyles",
        ""
    ],
    [
        "ext.advancedSearch.styles",
        ""
    ],
    [
        "ext.advancedSearch.searchtoken",
        "",
        [],
        1
    ],
    [
        "ext.advancedSearch.elements",
        "",
        [
            334,
            330,
            74,
            75,
            209,
            210
        ]
    ],
    [
        "ext.advancedSearch.init",
        "",
        [
            332,
            331
        ]
    ],
    [
        "ext.advancedSearch.SearchFieldUI",
        "",
        [
            193
        ]
    ],
    [
        "ext.abuseFilter",
        ""
    ],
    [
        "ext.abuseFilter.edit",
        "",
        [
            19,
            24,
            40,
            193
        ]
    ],
    [
        "ext.abuseFilter.tools",
        "",
        [
            19,
            38
        ]
    ],
    [
        "ext.abuseFilter.examine",
        "",
        [
            19,
            38
        ]
    ],
    [
        "ext.abuseFilter.ace",
        "",
        [
            551
        ]
    ],
    [
        "ext.abuseFilter.visualEditor",
        ""
    ],
    [
        "ext.abuseFilter.wikiEditor",
        "",
        [
            67,
            103,
            57
        ]
    ],
    [
        "pdfhandler.messages",
        ""
    ],
    [
        "ext.wikiEditor",
        "",
        [
            24,
            25,
            106,
            75,
            160,
            205,
            206,
            207,
            208,
            212,
            35
        ],
        3
    ],
    [
        "ext.wikiEditor.styles",
        "",
        [],
        3
    ],
    [
        "ext.wikiEditor.images",
        ""
    ],
    [
        "ext.wikiEditor.realtimepreview",
        "",
        [
            343,
            345,
            113,
            63,
            65,
            209
        ]
    ],
    [
        "ext.CodeMirror",
        "",
        [
            75
        ]
    ],
    [
        "ext.CodeMirror.WikiEditor",
        "",
        [
            347,
            24,
            25,
            208
        ]
    ],
    [
        "ext.CodeMirror.lib",
        ""
    ],
    [
        "ext.CodeMirror.addons",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.mode.mediawiki",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.css",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.javascript",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.xml",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.htmlmixed",
        "",
        [
            352,
            353,
            354
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.clike",
        "",
        [
            349
        ]
    ],
    [
        "ext.CodeMirror.lib.mode.php",
        "",
        [
            356,
            355
        ]
    ],
    [
        "ext.CodeMirror.visualEditor",
        "",
        [
            347,
            426
        ]
    ],
    [
        "ext.CodeMirror.v6.WikiEditor.init",
        ""
    ],
    [
        "ext.CodeMirror.v6.WikiEditor",
        "",
        [
            75
        ]
    ],
    [
        "ext.MassMessage.styles",
        ""
    ],
    [
        "ext.MassMessage.special.js",
        "",
        [
            17,
            103
        ]
    ],
    [
        "ext.MassMessage.content",
        "",
        [
            14,
            160,
            189
        ]
    ],
    [
        "ext.MassMessage.create",
        "",
        [
            40,
            52,
            160
        ]
    ],
    [
        "ext.MassMessage.edit",
        "",
        [
            40,
            165,
            189
        ]
    ],
    [
        "ext.betaFeatures",
        "",
        [
            190
        ]
    ],
    [
        "ext.betaFeatures.styles",
        ""
    ],
    [
        "mmv",
        "",
        [
            74,
            372
        ]
    ],
    [
        "mmv.ui.ondemandshareddependencies",
        "",
        [
            368,
            189
        ]
    ],
    [
        "mmv.ui.download.pane",
        "",
        [
            32,
            160,
            369
        ]
    ],
    [
        "mmv.ui.reuse.shareembed",
        "",
        [
            160,
            369
        ]
    ],
    [
        "mmv.bootstrap",
        "",
        [
            34,
            188,
            374
        ]
    ],
    [
        "mmv.bootstrap.autostart",
        "",
        [
            372
        ]
    ],
    [
        "mmv.head",
        "",
        [
            65,
            75
        ]
    ],
    [
        "ext.popups.icons",
        ""
    ],
    [
        "ext.popups.images",
        ""
    ],
    [
        "ext.popups",
        ""
    ],
    [
        "ext.popups.main",
        "",
        [
            34,
            376,
            74,
            81,
            65,
            75
        ]
    ],
    [
        "ext.linter.edit",
        "",
        [
            24
        ]
    ],
    [
        "socket.io",
        ""
    ],
    [
        "dompurify",
        ""
    ],
    [
        "color-picker",
        ""
    ],
    [
        "unicodejs",
        ""
    ],
    [
        "papaparse",
        ""
    ],
    [
        "rangefix",
        ""
    ],
    [
        "spark-md5",
        ""
    ],
    [
        "ext.visualEditor.supportCheck",
        "",
        [],
        4
    ],
    [
        "ext.visualEditor.sanitize",
        "",
        [
            381,
            407
        ],
        4
    ],
    [
        "ext.visualEditor.progressBarWidget",
        "",
        [],
        4
    ],
    [
        "ext.visualEditor.tempWikitextEditorWidget",
        "",
        [
            82,
            75
        ],
        4
    ],
    [
        "ext.visualEditor.desktopArticleTarget.init",
        "",
        [
            389,
            387,
            390,
            403,
            24,
            111,
            65
        ],
        4
    ],
    [
        "ext.visualEditor.desktopArticleTarget.noscript",
        ""
    ],
    [
        "ext.visualEditor.targetLoader",
        "",
        [
            406,
            403,
            24,
            65,
            75
        ],
        4
    ],
    [
        "ext.visualEditor.desktopTarget",
        "",
        [],
        4
    ],
    [
        "ext.visualEditor.desktopArticleTarget",
        "",
        [
            410,
            415,
            394,
            421
        ],
        4
    ],
    [
        "ext.visualEditor.mobileArticleTarget",
        "",
        [
            410,
            416
        ],
        4
    ],
    [
        "ext.visualEditor.collabTarget",
        "",
        [
            408,
            414,
            82,
            160,
            209,
            210
        ],
        4
    ],
    [
        "ext.visualEditor.collabTarget.desktop",
        "",
        [
            397,
            415,
            394,
            421
        ],
        4
    ],
    [
        "ext.visualEditor.collabTarget.mobile",
        "",
        [
            397,
            416,
            420
        ],
        4
    ],
    [
        "ext.visualEditor.collabTarget.init",
        "",
        [
            387,
            160,
            189
        ],
        4
    ],
    [
        "ext.visualEditor.collabTarget.init.styles",
        ""
    ],
    [
        "ext.visualEditor.ve",
        "",
        [],
        4
    ],
    [
        "ext.visualEditor.track",
        "",
        [
            402
        ],
        4
    ],
    [
        "ext.visualEditor.editCheck",
        "",
        [
            409
        ],
        4
    ],
    [
        "ext.visualEditor.core.utils",
        "",
        [
            403,
            189
        ],
        4
    ],
    [
        "ext.visualEditor.core.utils.parsing",
        "",
        [
            402
        ],
        4
    ],
    [
        "ext.visualEditor.base",
        "",
        [
            405,
            406,
            383
        ],
        4
    ],
    [
        "ext.visualEditor.mediawiki",
        "",
        [
            407,
            393,
            22,
            600
        ],
        4
    ],
    [
        "ext.visualEditor.mwsave",
        "",
        [
            419,
            17,
            19,
            42,
            43,
            209
        ],
        4
    ],
    [
        "ext.visualEditor.articleTarget",
        "",
        [
            420,
            409,
            94,
            162
        ],
        4
    ],
    [
        "ext.visualEditor.data",
        "",
        [
            408
        ]
    ],
    [
        "ext.visualEditor.core",
        "",
        [
            388,
            387,
            384,
            385,
            386
        ],
        4
    ],
    [
        "ext.visualEditor.commentAnnotation",
        "",
        [
            412
        ],
        4
    ],
    [
        "ext.visualEditor.rebase",
        "",
        [
            382,
            430,
            413,
            215,
            380
        ],
        4
    ],
    [
        "ext.visualEditor.core.desktop",
        "",
        [
            412
        ],
        4
    ],
    [
        "ext.visualEditor.core.mobile",
        "",
        [
            412
        ],
        4
    ],
    [
        "ext.visualEditor.welcome",
        "",
        [
            189
        ],
        4
    ],
    [
        "ext.visualEditor.switching",
        "",
        [
            189,
            201,
            204,
            206
        ],
        4
    ],
    [
        "ext.visualEditor.mwcore",
        "",
        [
            431,
            408,
            418,
            417,
            119,
            63,
            8,
            160
        ],
        4
    ],
    [
        "ext.visualEditor.mwextensions",
        "",
        [
            411,
            441,
            435,
            437,
            422,
            439,
            424,
            436,
            425,
            427
        ],
        4
    ],
    [
        "ext.visualEditor.mwextensions.desktop",
        "",
        [
            420,
            426,
            72
        ],
        4
    ],
    [
        "ext.visualEditor.mwformatting",
        "",
        [
            419
        ],
        4
    ],
    [
        "ext.visualEditor.mwimage.core",
        "",
        [
            419
        ],
        4
    ],
    [
        "ext.visualEditor.mwimage",
        "",
        [
            442,
            423,
            174,
            26,
            212
        ],
        4
    ],
    [
        "ext.visualEditor.mwlink",
        "",
        [
            419
        ],
        4
    ],
    [
        "ext.visualEditor.mwmeta",
        "",
        [
            425,
            96
        ],
        4
    ],
    [
        "ext.visualEditor.mwtransclusion",
        "",
        [
            419,
            177
        ],
        4
    ],
    [
        "treeDiffer",
        ""
    ],
    [
        "diffMatchPatch",
        ""
    ],
    [
        "ext.visualEditor.checkList",
        "",
        [
            412
        ],
        4
    ],
    [
        "ext.visualEditor.diffing",
        "",
        [
            429,
            412,
            428
        ],
        4
    ],
    [
        "ext.visualEditor.diffPage.init.styles",
        ""
    ],
    [
        "ext.visualEditor.diffLoader",
        "",
        [
            393
        ],
        4
    ],
    [
        "ext.visualEditor.diffPage.init",
        "",
        [
            433,
            432,
            189,
            201,
            204
        ],
        4
    ],
    [
        "ext.visualEditor.language",
        "",
        [
            412,
            600,
            105
        ],
        4
    ],
    [
        "ext.visualEditor.mwlanguage",
        "",
        [
            412
        ],
        4
    ],
    [
        "ext.visualEditor.mwalienextension",
        "",
        [
            419
        ],
        4
    ],
    [
        "ext.visualEditor.mwwikitext",
        "",
        [
            425,
            82
        ],
        4
    ],
    [
        "ext.visualEditor.mwgallery",
        "",
        [
            419,
            109,
            174,
            212
        ],
        4
    ],
    [
        "ext.visualEditor.mwsignature",
        "",
        [
            427
        ],
        4
    ],
    [
        "ext.visualEditor.icons",
        "",
        [
            443,
            444,
            202,
            203,
            204,
            206,
            207,
            208,
            209,
            210,
            213,
            214,
            215,
            200
        ],
        4
    ],
    [
        "ext.visualEditor.icons-licenses",
        ""
    ],
    [
        "ext.visualEditor.moduleIcons",
        ""
    ],
    [
        "ext.visualEditor.moduleIndicators",
        ""
    ],
    [
        "ext.citoid.visualEditor",
        "",
        [
            242,
            448,
            447
        ]
    ],
    [
        "quagga2",
        ""
    ],
    [
        "ext.citoid.visualEditor.icons",
        ""
    ],
    [
        "ext.citoid.visualEditor.data",
        "",
        [
            408
        ]
    ],
    [
        "ext.citoid.wikibase.init",
        ""
    ],
    [
        "ext.citoid.wikibase",
        "",
        [
            449,
            25,
            189
        ]
    ],
    [
        "ext.templateData",
        ""
    ],
    [
        "ext.templateDataGenerator.editPage",
        ""
    ],
    [
        "ext.templateDataGenerator.data",
        "",
        [
            187
        ]
    ],
    [
        "ext.templateDataGenerator.editTemplatePage.loading",
        ""
    ],
    [
        "ext.templateDataGenerator.editTemplatePage",
        "",
        [
            451,
            456,
            453,
            24,
            600,
            75,
            193,
            198,
            209,
            210,
            213
        ]
    ],
    [
        "ext.templateData.images",
        ""
    ],
    [
        "ext.TemplateWizard",
        "",
        [
            24,
            160,
            163,
            177,
            196,
            198,
            209
        ]
    ],
    [
        "ext.wikiLove.icon",
        ""
    ],
    [
        "ext.wikiLove.startup",
        "",
        [
            31
        ]
    ],
    [
        "ext.wikiLove.local",
        ""
    ],
    [
        "ext.wikiLove.init",
        "",
        [
            459
        ]
    ],
    [
        "mediawiki.libs.guiders",
        ""
    ],
    [
        "ext.guidedTour.styles",
        "",
        [
            462,
            157
        ]
    ],
    [
        "ext.guidedTour.lib.internal",
        "",
        [
            77
        ]
    ],
    [
        "ext.guidedTour.lib",
        "",
        [
            464,
            463,
            75
        ]
    ],
    [
        "ext.guidedTour.launcher",
        ""
    ],
    [
        "ext.guidedTour",
        "",
        [
            465
        ]
    ],
    [
        "ext.guidedTour.tour.firstedit",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.test",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.onshow",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.uprightdownleft",
        "",
        [
            467
        ]
    ],
    [
        "mobile.pagelist.styles",
        ""
    ],
    [
        "mobile.pagesummary.styles",
        ""
    ],
    [
        "mobile.userpage.styles",
        ""
    ],
    [
        "mobile.init.styles",
        ""
    ],
    [
        "mobile.init",
        "",
        [
            477
        ]
    ],
    [
        "mobile.startup",
        "",
        [
            34,
            112,
            188,
            65,
            36,
            475,
            472,
            473
        ]
    ],
    [
        "mobile.editor.overlay",
        "",
        [
            94,
            40,
            82,
            162,
            477,
            189,
            206
        ]
    ],
    [
        "mobile.mediaViewer",
        "",
        [
            477
        ]
    ],
    [
        "mobile.languages.structured",
        "",
        [
            477
        ]
    ],
    [
        "mobile.special.styles",
        ""
    ],
    [
        "mobile.special.watchlist.scripts",
        "",
        [
            477
        ]
    ],
    [
        "mobile.special.mobileoptions.styles",
        ""
    ],
    [
        "mobile.special.mobileoptions.scripts",
        "",
        [
            477
        ]
    ],
    [
        "mobile.special.userlogin.scripts",
        ""
    ],
    [
        "mobile.special.history.styles",
        ""
    ],
    [
        "mobile.special.pagefeed.styles",
        ""
    ],
    [
        "mobile.special.mobilediff.styles",
        ""
    ],
    [
        "skins.minerva.base.styles",
        ""
    ],
    [
        "skins.minerva.content.styles.images",
        ""
    ],
    [
        "skins.minerva.amc.styles",
        ""
    ],
    [
        "skins.minerva.overflow.icons",
        ""
    ],
    [
        "skins.minerva.icons.wikimedia",
        ""
    ],
    [
        "skins.minerva.mainPage.styles",
        ""
    ],
    [
        "skins.minerva.userpage.styles",
        ""
    ],
    [
        "skins.minerva.personalMenu.icons",
        ""
    ],
    [
        "skins.minerva.mainMenu.advanced.icons",
        ""
    ],
    [
        "skins.minerva.mainMenu.icons",
        ""
    ],
    [
        "skins.minerva.mainMenu.styles",
        ""
    ],
    [
        "skins.minerva.loggedin.styles",
        ""
    ],
    [
        "skins.minerva.scripts",
        "",
        [
            74,
            81,
            477,
            498,
            499,
            502
        ]
    ],
    [
        "skins.minerva.messageBox.styles",
        ""
    ],
    [
        "skins.minerva.categories.styles",
        ""
    ],
    [
        "ext.math.styles",
        ""
    ],
    [
        "ext.math.popup",
        "",
        [
            47
        ]
    ],
    [
        "mw.widgets.MathWbEntitySelector",
        "",
        [
            47,
            160,
            757,
            198
        ]
    ],
    [
        "ext.math.visualEditor",
        "",
        [
            504,
            419
        ]
    ],
    [
        "ext.math.visualEditor.mathSymbols",
        ""
    ],
    [
        "ext.math.visualEditor.chemSymbols",
        ""
    ],
    [
        "ext.babel",
        ""
    ],
    [
        "ext.vipsscaler",
        ""
    ],
    [
        "mediawiki.template.underscore",
        "",
        [
            513,
            35
        ]
    ],
    [
        "ext.pageTriage.external",
        ""
    ],
    [
        "ext.pageTriage.util",
        "",
        [
            513,
            74,
            75,
            26,
            30
        ]
    ],
    [
        "ext.pageTriage.views.toolbar",
        "",
        [
            518,
            514,
            19,
            25,
            882,
            202,
            512
        ]
    ],
    [
        "ext.pageTriage.newPagesFeed.vue",
        "",
        [
            31,
            514,
            19
        ]
    ],
    [
        "ext.pageTriage.defaultTagsOptions",
        "",
        [
            67
        ]
    ],
    [
        "ext.pageTriage.externalTagsOptions",
        "",
        [
            517
        ]
    ],
    [
        "ext.pageTriage.toolbarStartup",
        "",
        [
            513
        ]
    ],
    [
        "ext.pageTriage.article",
        "",
        [
            513,
            74,
            38
        ]
    ],
    [
        "ext.PageTriage.enqueue",
        "",
        [
            77
        ]
    ],
    [
        "ext.interwiki.specialpage",
        ""
    ],
    [
        "ext.echo.ui.desktop",
        "",
        [
            530,
            524
        ]
    ],
    [
        "ext.echo.ui",
        "",
        [
            525,
            871,
            193,
            202,
            203,
            206,
            209,
            213,
            214,
            215
        ]
    ],
    [
        "ext.echo.dm",
        "",
        [
            528,
            26
        ]
    ],
    [
        "ext.echo.api",
        "",
        [
            47
        ]
    ],
    [
        "ext.echo.mobile",
        "",
        [
            524,
            188
        ]
    ],
    [
        "ext.echo.init",
        "",
        [
            526
        ]
    ],
    [
        "ext.echo.centralauth",
        ""
    ],
    [
        "ext.echo.styles.badge",
        ""
    ],
    [
        "ext.echo.styles.notifications",
        ""
    ],
    [
        "ext.echo.styles.alert",
        ""
    ],
    [
        "ext.echo.special",
        "",
        [
            534,
            524
        ]
    ],
    [
        "ext.echo.styles.special",
        ""
    ],
    [
        "ext.thanks",
        "",
        [
            38,
            80
        ]
    ],
    [
        "ext.thanks.corethank",
        "",
        [
            535,
            14,
            198
        ]
    ],
    [
        "ext.thanks.mobilediff",
        "",
        [
            477
        ]
    ],
    [
        "ext.thanks.flowthank",
        "",
        [
            535,
            198
        ]
    ],
    [
        "ext.disambiguator",
        "",
        [
            38,
            57
        ]
    ],
    [
        "ext.disambiguator.visualEditor",
        "",
        [
            426
        ]
    ],
    [
        "ext.discussionTools.init.styles",
        ""
    ],
    [
        "ext.discussionTools.debug.styles",
        ""
    ],
    [
        "ext.discussionTools.init",
        "",
        [
            541,
            544,
            406,
            65,
            75,
            26,
            198,
            385
        ]
    ],
    [
        "ext.discussionTools.minervaicons",
        ""
    ],
    [
        "ext.discussionTools.debug",
        "",
        [
            543
        ]
    ],
    [
        "ext.discussionTools.ReplyWidget",
        "",
        [
            862,
            543,
            410,
            440,
            438,
            165
        ]
    ],
    [
        "ext.codeEditor",
        "",
        [
            549
        ],
        3
    ],
    [
        "ext.codeEditor.styles",
        ""
    ],
    [
        "jquery.codeEditor",
        "",
        [
            551,
            550,
            343,
            198
        ],
        3
    ],
    [
        "ext.codeEditor.icons",
        ""
    ],
    [
        "ext.codeEditor.ace",
        "",
        [],
        5
    ],
    [
        "ext.codeEditor.ace.modes",
        "",
        [
            551
        ],
        5
    ],
    [
        "ext.scribunto.errors",
        "",
        [
            193
        ]
    ],
    [
        "ext.scribunto.logs",
        ""
    ],
    [
        "ext.scribunto.edit",
        "",
        [
            19,
            38
        ]
    ],
    [
        "ext.relatedArticles.styles",
        ""
    ],
    [
        "ext.relatedArticles.readMore.bootstrap",
        "",
        [
            74,
            75
        ]
    ],
    [
        "ext.relatedArticles.readMore",
        "",
        [
            77,
            187
        ]
    ],
    [
        "ext.RevisionSlider.lazyCss",
        ""
    ],
    [
        "ext.RevisionSlider.lazyJs",
        "",
        [
            563,
            214
        ]
    ],
    [
        "ext.RevisionSlider.init",
        "",
        [
            563,
            564,
            213
        ]
    ],
    [
        "ext.RevisionSlider.noscript",
        ""
    ],
    [
        "ext.RevisionSlider.Settings",
        "",
        [
            65,
            75
        ]
    ],
    [
        "ext.RevisionSlider.Slider",
        "",
        [
            565,
            25,
            74,
            26,
            189,
            209,
            214
        ]
    ],
    [
        "ext.RevisionSlider.dialogImages",
        ""
    ],
    [
        "ext.TwoColConflict.SplitJs",
        "",
        [
            568,
            569,
            63,
            65,
            75,
            189,
            209
        ]
    ],
    [
        "ext.TwoColConflict.SplitCss",
        ""
    ],
    [
        "ext.TwoColConflict.Split.TourImages",
        ""
    ],
    [
        "ext.TwoColConflict.Util",
        ""
    ],
    [
        "ext.TwoColConflict.JSCheck",
        ""
    ],
    [
        "ext.eventLogging",
        "",
        [
            75
        ]
    ],
    [
        "ext.eventLogging.debug",
        ""
    ],
    [
        "ext.eventLogging.jsonSchema",
        ""
    ],
    [
        "ext.eventLogging.jsonSchema.styles",
        ""
    ],
    [
        "ext.wikimediaEvents",
        "",
        [
            571,
            74,
            81,
            65,
            83
        ]
    ],
    [
        "ext.wikimediaEvents.specialPages",
        "",
        [
            571
        ]
    ],
    [
        "ext.wikimediaEvents.wikibase",
        "",
        [
            571,
            81
        ]
    ],
    [
        "ext.wikimediaEvents.networkprobe",
        "",
        [
            571
        ]
    ],
    [
        "ext.navigationTiming",
        "",
        [
            571
        ]
    ],
    [
        "ext.uls.common",
        "",
        [
            600,
            65,
            75
        ]
    ],
    [
        "ext.uls.compactlinks",
        "",
        [
            580
        ]
    ],
    [
        "ext.uls.ime",
        "",
        [
            590,
            598
        ]
    ],
    [
        "ext.uls.displaysettings",
        "",
        [
            582,
            589
        ]
    ],
    [
        "ext.uls.geoclient",
        "",
        [
            80
        ]
    ],
    [
        "ext.uls.i18n",
        "",
        [
            16,
            77
        ]
    ],
    [
        "ext.uls.interface",
        "",
        [
            596,
            187
        ]
    ],
    [
        "ext.uls.interlanguage",
        ""
    ],
    [
        "ext.uls.languagenames",
        ""
    ],
    [
        "ext.uls.languagesettings",
        "",
        [
            591,
            592,
            601
        ]
    ],
    [
        "ext.uls.mediawiki",
        "",
        [
            580,
            588,
            591,
            596,
            599
        ]
    ],
    [
        "ext.uls.messages",
        "",
        [
            585
        ]
    ],
    [
        "ext.uls.preferences",
        "",
        [
            65,
            75
        ]
    ],
    [
        "ext.uls.preferencespage",
        ""
    ],
    [
        "ext.uls.pt",
        ""
    ],
    [
        "ext.uls.setlang",
        "",
        [
            31,
            74
        ]
    ],
    [
        "ext.uls.webfonts",
        "",
        [
            592
        ]
    ],
    [
        "ext.uls.webfonts.repository",
        ""
    ],
    [
        "jquery.ime",
        ""
    ],
    [
        "jquery.uls",
        "",
        [
            16,
            600,
            601
        ]
    ],
    [
        "jquery.uls.data",
        ""
    ],
    [
        "jquery.uls.grid",
        ""
    ],
    [
        "rangy.core",
        ""
    ],
    [
        "ext.cx.contributions",
        "",
        [
            190,
            203,
            204
        ]
    ],
    [
        "ext.cx.model",
        ""
    ],
    [
        "ext.cx.dashboard",
        "",
        [
            632,
            21,
            160,
            26,
            610,
            642,
            611,
            206,
            212,
            213
        ]
    ],
    [
        "sx.publishing.followup",
        "",
        [
            610,
            609,
            27
        ]
    ],
    [
        "mw.cx3",
        "",
        [
            610,
            609
        ]
    ],
    [
        "mw.cx3.ve",
        "",
        [
            242,
            396
        ]
    ],
    [
        "mw.cx.util",
        "",
        [
            604,
            75
        ]
    ],
    [
        "mw.cx.SiteMapper",
        "",
        [
            604,
            47,
            75
        ]
    ],
    [
        "mw.cx.ui.LanguageFilter",
        "",
        [
            590,
            157,
            636,
            609,
            209
        ]
    ],
    [
        "ext.cx.wikibase.link",
        ""
    ],
    [
        "ext.cx.uls.quick.actions",
        "",
        [
            580,
            586,
            610,
            209
        ]
    ],
    [
        "ext.cx.eventlogging.campaigns",
        "",
        [
            75
        ]
    ],
    [
        "ext.cx.interlanguagelink.init",
        "",
        [
            580
        ]
    ],
    [
        "ext.cx.interlanguagelink",
        "",
        [
            580,
            610,
            193,
            209
        ]
    ],
    [
        "ext.cx.translation.conflict",
        "",
        [
            103
        ]
    ],
    [
        "ext.cx.stats",
        "",
        [
            619,
            633,
            632,
            600,
            26,
            610
        ]
    ],
    [
        "chart.js",
        ""
    ],
    [
        "ext.cx.entrypoints.recentedit",
        "",
        [
            600,
            610,
            609,
            27
        ]
    ],
    [
        "ext.cx.entrypoints.recenttranslation",
        "",
        [
            600,
            610,
            609,
            27
        ]
    ],
    [
        "ext.cx.entrypoints.newarticle",
        "",
        [
            633,
            157,
            190
        ]
    ],
    [
        "ext.cx.entrypoints.newarticle.veloader",
        ""
    ],
    [
        "ext.cx.entrypoints.languagesearcher.init",
        ""
    ],
    [
        "ext.cx.entrypoints.languagesearcher",
        "",
        [
            600,
            610
        ]
    ],
    [
        "ext.cx.entrypoints.mffrequentlanguages",
        "",
        [
            610
        ]
    ],
    [
        "ext.cx.entrypoints.ulsrelevantlanguages",
        "",
        [
            580,
            610,
            27
        ]
    ],
    [
        "ext.cx.entrypoints.newbytranslation",
        "",
        [
            610,
            609,
            193,
            203,
            209
        ]
    ],
    [
        "ext.cx.entrypoints.newbytranslation.mobile",
        "",
        [
            610,
            609,
            203
        ]
    ],
    [
        "ext.cx.betafeature.init",
        ""
    ],
    [
        "ext.cx.entrypoints.contributionsmenu",
        "",
        [
            633,
            103,
            159
        ]
    ],
    [
        "ext.cx.widgets.spinner",
        "",
        [
            604
        ]
    ],
    [
        "ext.cx.widgets.callout",
        ""
    ],
    [
        "mw.cx.dm",
        "",
        [
            604,
            187
        ]
    ],
    [
        "mw.cx.dm.Translation",
        "",
        [
            634
        ]
    ],
    [
        "mw.cx.ui",
        "",
        [
            604,
            189
        ]
    ],
    [
        "mw.cx.visualEditor",
        "",
        [
            242,
            415,
            394,
            421,
            638,
            639
        ]
    ],
    [
        "ve.ce.CXLintableNode",
        "",
        [
            412
        ]
    ],
    [
        "ve.dm.CXLintableNode",
        "",
        [
            412,
            634
        ]
    ],
    [
        "mw.cx.init",
        "",
        [
            632,
            426,
            646,
            642,
            638,
            639,
            641
        ]
    ],
    [
        "ve.init.mw.CXTarget",
        "",
        [
            415,
            610,
            635,
            636,
            609
        ]
    ],
    [
        "mw.cx.ui.Infobar",
        "",
        [
            636,
            609,
            202,
            209
        ]
    ],
    [
        "mw.cx.ui.CaptchaDialog",
        "",
        [
            873,
            636
        ]
    ],
    [
        "mw.cx.ui.LoginDialog",
        "",
        [
            636
        ]
    ],
    [
        "mw.cx.tools.InstructionsTool",
        "",
        [
            646,
            36
        ]
    ],
    [
        "mw.cx.tools.TranslationTool",
        "",
        [
            636
        ]
    ],
    [
        "mw.cx.ui.FeatureDiscoveryWidget",
        "",
        [
            63,
            636
        ]
    ],
    [
        "mw.cx.skin",
        ""
    ],
    [
        "mw.externalguidance.init",
        "",
        [
            74
        ]
    ],
    [
        "mw.externalguidance",
        "",
        [
            47,
            477,
            651,
            206
        ]
    ],
    [
        "mw.externalguidance.icons",
        ""
    ],
    [
        "mw.externalguidance.special",
        "",
        [
            32,
            600,
            47,
            477,
            651
        ]
    ],
    [
        "wikibase.client.init",
        ""
    ],
    [
        "wikibase.client.miscStyles",
        ""
    ],
    [
        "wikibase.client.vector-2022",
        ""
    ],
    [
        "wikibase.client.linkitem.init",
        "",
        [
            19
        ]
    ],
    [
        "jquery.wikibase.linkitem",
        "",
        [
            19,
            25,
            47,
            757,
            756,
            874
        ]
    ],
    [
        "wikibase.client.action.edit.collapsibleFooter",
        "",
        [
            18,
            55,
            65
        ]
    ],
    [
        "ext.wikimediaBadges",
        ""
    ],
    [
        "ext.TemplateSandbox.top",
        ""
    ],
    [
        "ext.TemplateSandbox",
        "",
        [
            660
        ]
    ],
    [
        "ext.TemplateSandbox.visualeditor",
        "",
        [
            160,
            189
        ]
    ],
    [
        "ext.pageassessments.special",
        "",
        [
            21,
            190
        ]
    ],
    [
        "ext.jsonConfig",
        ""
    ],
    [
        "ext.jsonConfig.edit",
        "",
        [
            24,
            175,
            198
        ]
    ],
    [
        "ext.MWOAuth.styles",
        ""
    ],
    [
        "ext.MWOAuth.AuthorizeDialog",
        "",
        [
            198
        ]
    ],
    [
        "ext.oath.totp.showqrcode.styles",
        ""
    ],
    [
        "ext.webauthn.ui.base",
        "",
        [
            189
        ]
    ],
    [
        "ext.webauthn.register",
        "",
        [
            669
        ]
    ],
    [
        "ext.webauthn.login",
        "",
        [
            669
        ]
    ],
    [
        "ext.webauthn.manage",
        "",
        [
            669
        ]
    ],
    [
        "ext.webauthn.disable",
        "",
        [
            669
        ]
    ],
    [
        "ext.ores.highlighter",
        ""
    ],
    [
        "ext.ores.styles",
        ""
    ],
    [
        "ext.ores.api",
        ""
    ],
    [
        "ext.checkUser.clientHints",
        "",
        [
            38,
            11
        ]
    ],
    [
        "ext.checkUser",
        "",
        [
            22,
            74,
            61,
            65,
            75,
            160,
            177,
            206,
            209,
            211,
            213,
            215
        ]
    ],
    [
        "ext.checkUser.styles",
        ""
    ],
    [
        "ext.ipInfo",
        "",
        [
            51,
            65,
            75,
            193,
            203
        ]
    ],
    [
        "ext.ipInfo.styles",
        ""
    ],
    [
        "ext.kartographer",
        ""
    ],
    [
        "ext.kartographer.style",
        ""
    ],
    [
        "ext.kartographer.site",
        ""
    ],
    [
        "mapbox",
        ""
    ],
    [
        "leaflet.draw",
        "",
        [
            685
        ]
    ],
    [
        "ext.kartographer.link",
        "",
        [
            689,
            188
        ]
    ],
    [
        "ext.kartographer.box",
        "",
        [
            690,
            701,
            684,
            683,
            693,
            74,
            38,
            212
        ]
    ],
    [
        "ext.kartographer.linkbox",
        "",
        [
            693
        ]
    ],
    [
        "ext.kartographer.data",
        ""
    ],
    [
        "ext.kartographer.dialog",
        "",
        [
            685,
            188,
            193,
            198
        ]
    ],
    [
        "ext.kartographer.dialog.sidebar",
        "",
        [
            65,
            209,
            214
        ]
    ],
    [
        "ext.kartographer.util",
        "",
        [
            682
        ]
    ],
    [
        "ext.kartographer.frame",
        "",
        [
            688,
            188
        ]
    ],
    [
        "ext.kartographer.staticframe",
        "",
        [
            689,
            188,
            212
        ]
    ],
    [
        "ext.kartographer.preview",
        ""
    ],
    [
        "ext.kartographer.editing",
        "",
        [
            38
        ]
    ],
    [
        "ext.kartographer.editor",
        "",
        [
            688,
            686
        ]
    ],
    [
        "ext.kartographer.visualEditor",
        "",
        [
            693,
            419,
            211
        ]
    ],
    [
        "ext.kartographer.lib.leaflet.markercluster",
        "",
        [
            685
        ]
    ],
    [
        "ext.kartographer.lib.topojson",
        "",
        [
            685
        ]
    ],
    [
        "ext.kartographer.wv",
        "",
        [
            685,
            206
        ]
    ],
    [
        "ext.kartographer.specialMap",
        ""
    ],
    [
        "ext.pageviewinfo",
        "",
        [
            "ext.graph.render",
            189
        ]
    ],
    [
        "ext.3d",
        "",
        [
            19
        ]
    ],
    [
        "ext.3d.styles",
        ""
    ],
    [
        "mmv.3d",
        "",
        [
            705,
            368
        ]
    ],
    [
        "mmv.3d.head",
        "",
        [
            705,
            190,
            201,
            203
        ]
    ],
    [
        "ext.3d.special.upload",
        "",
        [
            710,
            144
        ]
    ],
    [
        "ext.3d.special.upload.styles",
        ""
    ],
    [
        "special.readinglist.styles",
        ""
    ],
    [
        "special.readinglist.scripts",
        "",
        [
            31,
            75
        ]
    ],
    [
        "ext.GlobalPreferences.global",
        "",
        [
            160,
            168,
            178
        ]
    ],
    [
        "ext.GlobalPreferences.local",
        ""
    ],
    [
        "ext.GlobalPreferences.global-nojs",
        ""
    ],
    [
        "ext.GlobalPreferences.local-nojs",
        ""
    ],
    [
        "ext.growthExperiments.mobileMenu.icons",
        ""
    ],
    [
        "ext.growthExperiments.SuggestedEditSession",
        "",
        [
            74,
            65,
            75,
            187
        ]
    ],
    [
        "ext.growthExperiments.LevelingUp.InviteToSuggestedEdits",
        "",
        [
            75,
            190,
            214
        ]
    ],
    [
        "ext.growthExperiments.HelpPanelCta.styles",
        ""
    ],
    [
        "ext.growthExperiments.HomepageDiscovery.styles",
        ""
    ],
    [
        "ext.growthExperiments.HomepageDiscovery",
        ""
    ],
    [
        "ext.growthExperiments.Homepage.mobile",
        "",
        [
            726,
            477
        ]
    ],
    [
        "ext.growthExperiments.Homepage",
        "",
        [
            74,
            75,
            198
        ]
    ],
    [
        "ext.growthExperiments.Homepage.NewImpact",
        "",
        [
            31,
            75,
            26
        ]
    ],
    [
        "ext.growthExperiments.Homepage.Mentorship",
        "",
        [
            733,
            718,
            188
        ]
    ],
    [
        "ext.growthExperiments.Homepage.SuggestedEdits",
        "",
        [
            744,
            718,
            63,
            188,
            193,
            198,
            203,
            206,
            212
        ]
    ],
    [
        "ext.growthExperiments.Homepage.styles",
        ""
    ],
    [
        "ext.growthExperiments.StructuredTask",
        "",
        [
            732,
            739,
            425,
            188,
            212,
            213,
            214
        ]
    ],
    [
        "ext.growthExperiments.StructuredTask.desktop",
        "",
        [
            729,
            395
        ]
    ],
    [
        "ext.growthExperiments.StructuredTask.mobile",
        "",
        [
            729,
            396
        ]
    ],
    [
        "ext.growthExperiments.StructuredTask.PreEdit",
        "",
        [
            744,
            718,
            193,
            198
        ]
    ],
    [
        "ext.growthExperiments.Help",
        "",
        [
            744,
            739,
            74,
            65,
            193,
            198,
            202,
            204,
            205,
            206,
            209,
            215
        ]
    ],
    [
        "ext.growthExperiments.HelpPanel",
        "",
        [
            733,
            720,
            732,
            63,
            214
        ]
    ],
    [
        "ext.growthExperiments.HelpPanel.init",
        "",
        [
            718
        ]
    ],
    [
        "ext.growthExperiments.PostEdit",
        "",
        [
            744,
            718,
            739,
            198,
            212,
            214
        ]
    ],
    [
        "ext.growthExperiments.Account",
        "",
        [
            188,
            193
        ]
    ],
    [
        "ext.growthExperiments.Account.styles",
        ""
    ],
    [
        "ext.growthExperiments.icons",
        ""
    ],
    [
        "ext.growthExperiments.MentorDashboard",
        "",
        [
            31,
            739,
            105,
            177,
            26,
            198,
            205,
            206,
            209,
            212,
            213,
            214,
            215,
            29
        ]
    ],
    [
        "ext.growthExperiments.MentorDashboard.styles",
        ""
    ],
    [
        "ext.growthExperiments.MentorDashboard.Discovery",
        "",
        [
            63
        ]
    ],
    [
        "ext.growthExperiments.MentorDashboard.PostEdit",
        "",
        [
            57
        ]
    ],
    [
        "ext.growthExperiments.DataStore",
        "",
        [
            75,
            190
        ]
    ],
    [
        "ext.growthExperiments.MidEditSignup",
        "",
        [
            65,
            198
        ]
    ],
    [
        "ext.nearby.styles",
        ""
    ],
    [
        "ext.nearby.scripts",
        "",
        [
            31,
            748,
            188
        ]
    ],
    [
        "ext.nearby.images",
        ""
    ],
    [
        "ext.phonos.init",
        ""
    ],
    [
        "ext.phonos",
        "",
        [
            751,
            749,
            752,
            190,
            194,
            212
        ]
    ],
    [
        "ext.phonos.icons.js",
        ""
    ],
    [
        "ext.phonos.styles",
        ""
    ],
    [
        "ext.phonos.icons",
        ""
    ],
    [
        "ext.parsermigration.edit",
        ""
    ],
    [
        "mw.config.values.wbCurrentSiteDetails",
        ""
    ],
    [
        "mw.config.values.wbSiteDetails",
        ""
    ],
    [
        "mw.config.values.wbRepo",
        ""
    ],
    [
        "ext.gadget.modrollback",
        "",
        [],
        2
    ],
    [
        "ext.gadget.confirmationRollback-mobile",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.removeAccessKeys",
        "",
        [
            3,
            77
        ],
        2
    ],
    [
        "ext.gadget.searchFocus",
        "",
        [],
        2
    ],
    [
        "ext.gadget.GoogleTrans",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ImageAnnotator",
        "",
        [],
        2
    ],
    [
        "ext.gadget.imagelinks",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.Navigation_popups",
        "",
        [
            75
        ],
        2
    ],
    [
        "ext.gadget.exlinks",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.search-new-tab",
        "",
        [],
        2
    ],
    [
        "ext.gadget.PrintOptions",
        "",
        [],
        2
    ],
    [
        "ext.gadget.revisionjumper",
        "",
        [],
        2
    ],
    [
        "ext.gadget.Twinkle",
        "",
        [
            771,
            773
        ],
        6
    ],
    [
        "ext.gadget.morebits",
        "",
        [
            75,
            25
        ],
        6
    ],
    [
        "ext.gadget.Twinkle-pagestyles",
        "",
        [],
        2
    ],
    [
        "ext.gadget.select2",
        "",
        [],
        2
    ],
    [
        "ext.gadget.HideCentralNotice",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ReferenceTooltips",
        "",
        [
            80,
            13
        ],
        2
    ],
    [
        "ext.gadget.formWizard",
        "",
        [],
        2
    ],
    [
        "ext.gadget.formWizard-core",
        "",
        [
            154,
            75,
            12,
            25
        ],
        2
    ],
    [
        "ext.gadget.responsiveContentBase",
        "",
        [],
        2
    ],
    [
        "ext.gadget.Prosesize",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.find-archived-section",
        "",
        [],
        2
    ],
    [
        "ext.gadget.geonotice",
        "",
        [],
        2
    ],
    [
        "ext.gadget.geonotice-core",
        "",
        [
            65
        ],
        2
    ],
    [
        "ext.gadget.watchlist-notice",
        "",
        [],
        2
    ],
    [
        "ext.gadget.watchlist-notice-core",
        "",
        [
            65
        ],
        2
    ],
    [
        "ext.gadget.WatchlistBase",
        "",
        [],
        2
    ],
    [
        "ext.gadget.WatchlistGreenIndicators",
        "",
        [],
        2
    ],
    [
        "ext.gadget.WatchlistChangesBold",
        "",
        [],
        2
    ],
    [
        "ext.gadget.SubtleUpdatemarker",
        "",
        [],
        2
    ],
    [
        "ext.gadget.defaultsummaries",
        "",
        [
            190
        ],
        2
    ],
    [
        "ext.gadget.citations",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.DotsSyntaxHighlighter",
        "",
        [],
        2
    ],
    [
        "ext.gadget.HotCat",
        "",
        [],
        2
    ],
    [
        "ext.gadget.wikEdDiff",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ProveIt",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ProveIt-classic",
        "",
        [
            25,
            24,
            77
        ],
        2
    ],
    [
        "ext.gadget.Shortdesc-helper",
        "",
        [
            38,
            798
        ],
        2
    ],
    [
        "ext.gadget.Shortdesc-helper-pagestyles-vector",
        "",
        [],
        2
    ],
    [
        "ext.gadget.libSettings",
        "",
        [
            5
        ],
        2
    ],
    [
        "ext.gadget.wikEd",
        "",
        [
            24,
            5
        ],
        2
    ],
    [
        "ext.gadget.afchelper",
        "",
        [
            75,
            12,
            19,
            25
        ],
        2
    ],
    [
        "ext.gadget.charinsert",
        "",
        [],
        2
    ],
    [
        "ext.gadget.charinsert-core",
        "",
        [
            24,
            3,
            65
        ],
        2
    ],
    [
        "ext.gadget.legacyToolbar",
        "",
        [],
        2
    ],
    [
        "ext.gadget.extra-toolbar-buttons",
        "",
        [],
        2
    ],
    [
        "ext.gadget.extra-toolbar-buttons-core",
        "",
        [],
        2
    ],
    [
        "ext.gadget.refToolbar",
        "",
        [
            5,
            77
        ],
        2
    ],
    [
        "ext.gadget.refToolbarBase",
        "",
        [],
        2
    ],
    [
        "ext.gadget.edittop",
        "",
        [
            5,
            77
        ],
        2
    ],
    [
        "ext.gadget.UTCLiveClock",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.UTCLiveClock-pagestyles",
        "",
        [],
        2
    ],
    [
        "ext.gadget.purgetab",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.ExternalSearch",
        "",
        [],
        2
    ],
    [
        "ext.gadget.CollapsibleNav",
        "",
        [
            18,
            65
        ],
        2
    ],
    [
        "ext.gadget.MenuTabsToggle",
        "",
        [
            80
        ],
        2
    ],
    [
        "ext.gadget.dropdown-menus",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.dropdown-menus-pagestyles",
        "",
        [],
        2
    ],
    [
        "ext.gadget.addsection-plus",
        "",
        [],
        2
    ],
    [
        "ext.gadget.CommentsInLocalTime",
        "",
        [],
        2
    ],
    [
        "ext.gadget.OldDiff",
        "",
        [],
        2
    ],
    [
        "ext.gadget.NoAnimations",
        "",
        [],
        2
    ],
    [
        "ext.gadget.disablesuggestions",
        "",
        [],
        2
    ],
    [
        "ext.gadget.NoSmallFonts",
        "",
        [],
        2
    ],
    [
        "ext.gadget.topalert",
        "",
        [],
        2
    ],
    [
        "ext.gadget.metadata",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.JustifyParagraphs",
        "",
        [],
        2
    ],
    [
        "ext.gadget.righteditlinks",
        "",
        [],
        2
    ],
    [
        "ext.gadget.PrettyLog",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.switcher",
        "",
        [],
        2
    ],
    [
        "ext.gadget.SidebarTranslate",
        "",
        [],
        2
    ],
    [
        "ext.gadget.Blackskin",
        "",
        [],
        2
    ],
    [
        "ext.gadget.dark-mode-toggle",
        "",
        [
            38,
            74,
            65
        ],
        2
    ],
    [
        "ext.gadget.dark-mode-toggle-pagestyles",
        "",
        [],
        2
    ],
    [
        "ext.gadget.VectorClassic",
        "",
        [],
        2
    ],
    [
        "ext.gadget.widensearch",
        "",
        [],
        2
    ],
    [
        "ext.gadget.DisambiguationLinks",
        "",
        [],
        2
    ],
    [
        "ext.gadget.markblocked",
        "",
        [
            111
        ],
        2
    ],
    [
        "ext.gadget.responsiveContent",
        "",
        [],
        2
    ],
    [
        "ext.gadget.HideInterwikiSearchResults",
        "",
        [],
        2
    ],
    [
        "ext.gadget.XTools-ArticleInfo",
        "",
        [],
        2
    ],
    [
        "ext.gadget.RegexMenuFramework",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ShowMessageNames",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.DebugMode",
        "",
        [
            77
        ],
        2
    ],
    [
        "ext.gadget.contribsrange",
        "",
        [
            77,
            19
        ],
        2
    ],
    [
        "ext.gadget.BugStatusUpdate",
        "",
        [],
        2
    ],
    [
        "ext.gadget.RTRC",
        "",
        [],
        2
    ],
    [
        "ext.gadget.script-installer",
        "",
        [
            157
        ],
        2
    ],
    [
        "ext.gadget.XFDcloser",
        "",
        [
            75
        ],
        2
    ],
    [
        "ext.gadget.XFDcloser-core",
        "",
        [
            193,
            198,
            209,
            203,
            213,
            202
        ],
        2
    ],
    [
        "ext.gadget.XFDcloser-core-beta",
        "",
        [
            193,
            198,
            209,
            203,
            213,
            202
        ],
        2
    ],
    [
        "ext.gadget.libExtraUtil",
        "",
        [],
        2
    ],
    [
        "ext.gadget.mobile-sidebar",
        "",
        [],
        2
    ],
    [
        "ext.gadget.addMe",
        "",
        [],
        2
    ],
    [
        "ext.gadget.NewImageThumb",
        "",
        [],
        2
    ],
    [
        "ext.gadget.StickyTableHeaders",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ShowJavascriptErrors",
        "",
        [],
        2
    ],
    [
        "ext.gadget.PageDescriptions",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.autonum",
        "",
        [],
        2
    ],
    [
        "ext.gadget.libLua",
        "",
        [
            38
        ],
        2
    ],
    [
        "ext.gadget.libSensitiveIPs",
        "",
        [
            858
        ],
        2
    ],
    [
        "ext.gadget.dark-mode",
        "",
        [],
        2
    ],
    [
        "ext.gadget.ondemand-WikiMiniAtlas",
        "",
        [],
        2
    ],
    [
        "ext.confirmEdit.CaptchaInputWidget",
        "",
        [
            190
        ]
    ],
    [
        "ext.globalCssJs.user",
        "",
        [],
        0,
        "metawiki"
    ],
    [
        "ext.globalCssJs.user.styles",
        "",
        [],
        0,
        "metawiki"
    ],
    [
        "ext.guidedTour.tour.RcFiltersIntro",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.WlFiltersIntro",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.RcFiltersHighlight",
        "",
        [
            467
        ]
    ],
    [
        "ext.wikimediaMessages.ipInfo.hooks",
        "",
        [
            680,
            209
        ]
    ],
    [
        "ext.guidedTour.tour.firsteditve",
        "",
        [
            467
        ]
    ],
    [
        "ext.echo.emailicons",
        ""
    ],
    [
        "ext.echo.secondaryicons",
        ""
    ],
    [
        "ext.wikimediaEvents.visualEditor",
        "",
        [
            393
        ]
    ],
    [
        "mw.cx.externalmessages",
        ""
    ],
    [
        "wikibase.Site",
        "",
        [
            590
        ]
    ],
    [
        "ext.guidedTour.tour.checkuserinvestigateform",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.checkuserinvestigate",
        "",
        [
            678,
            467
        ]
    ],
    [
        "ext.guidedTour.tour.helppanel",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.homepage_mentor",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.homepage_welcome",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.homepage_discovery",
        "",
        [
            467
        ]
    ],
    [
        "ext.guidedTour.tour.newimpact_discovery",
        "",
        [
            467
        ]
    ],
    [
        "mediawiki.messagePoster",
        "",
        [
            47
        ]
    ]
]);

		// First set page-specific config needed by mw.loader (wgUserName)
		mw.config.set( window.RLCONF || {} );
		mw.loader.state( window.RLSTATE || {} );
		mw.loader.load( window.RLPAGEMODULES || [] );

		// Process RLQ callbacks
		//
		// The code in these callbacks could've been exposed from load.php and
		// requested client-side. Instead, they are pushed by the server directly
		// (from ResourceLoaderClientHtml and other parts of MediaWiki). This
		// saves the need for additional round trips. It also allows load.php
		// to remain stateless and sending personal data in the HTML instead.
		//
		// The HTML inline script lazy-defines the 'RLQ' array. Now that we are
		// processing it, replace it with an implementation where 'push' actually
		// considers executing the code directly. This is to ensure any late
		// arrivals will also be processed. Late arrival can happen because
		// startup.js is executed asynchronously, concurrently with the streaming
		// response of the HTML.
		queue = window.RLQ || [];
		// Replace RLQ with an empty array, then process the things that were
		// in RLQ previously. We have to do this to avoid an infinite loop:
		// non-function items are added back to RLQ by the processing step.
		RLQ = [];
		RLQ.push = function ( fn ) {
			if ( typeof fn === 'function' ) {
				fn();
			} else {
				// If the first parameter is not a function, then it is an array
				// containing a list of required module names and a function.
				// Do an actual push for now, as this signature is handled
				// later by mediawiki.base.js.
				RLQ[ RLQ.length ] = fn;
			}
		};
		while ( queue[ 0 ] ) {
			// Process all values gathered so far
			RLQ.push( queue.shift() );
		}

		// Clear and disable the basic (Grade C) queue.
		NORLQ = {
			push: function () {}
		};
	}() );
}
mw.loader.state({
    "startup": "ready"
});