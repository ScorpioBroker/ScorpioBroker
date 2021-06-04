# Contributing to the ScorpioBroker

This document describes the guidelines to contribute to the Scorpio NGSI-LD Broker. If you are
planning to contribute to the code you should read this document and get familiar with its content.

## sign up your contributor agreement license

If you are interested to make any contribution to the ScorpioBroker, please download the contributor license agreement listed below and send us you signed agreement via email scorpio-support@listserv.neclab.eu  


[ScorpioBroker Entity Contributor License Agreement](https://github.com/ScorpioBroker/ScorpioBroker/blob/development/ScorpioBroker-Entity_CLA_final.pdf)


[ScorpioBroker Individual Contributor License Agreement](https://github.com/ScorpioBroker/ScorpioBroker/blob/development/ScorpioBroker-Individual_CLA_final.pdf)

## Ground rules & expectations

Before we get started, here are a few things we expect from you (and that you should expect from others):

* Be kind and thoughtful in your conversations around this project. We all come from different backgrounds and
  projects, which means we likely have different perspectives on "how open source is done." Try to listen to others
  rather than convince them that your way is correct.
* Please ensure that your contribution passes all tests. If there are test failures, you will need to address them
  before we can merge your contribution.
* When adding content, please consider if it is widely valuable. Please don't add references or links to things you or
  your employer have created as others will do so if they appreciate it.
* When reporting a vulnerability on the software, please, put in contact with Scorpio repo maintainers in order to discuss it 
  in a private way.

## General principles

* Scorpio Brokers programming language is Java and it uses Postgres and Postgis specific SQL queries.
* Scorpio uses Maven as dependency management and build system. More details in our [dependency guidelines](#dependency-guidelines)
* Some tools we provide are written in Python. These tools should try to follow best practices as well. However we are well aware that
  the "Pythonic way" and Java best practices do not always match up so we are not as strict here.
* Generaly we try to stay in line with Java Best Practices when there is no GOOD reason to not. We are aware that there is some best 
  practices which are a matter discussion in the community. This can always be discussed in a PR.
* Common sense efficientiancy in the code is expected. (E.g. multiple loops through the same structure is not acceptable) 
* Simple code (i.e. cleaner and shorter) is preferred upon complex code. However there is always a trade off between performance and maintainability.
  Our guideline here comes in form of an Einstein quote: “Everything should be made as simple as possible, but no simpler.”
* Code contributed to Scorpio must follow the [code style guidelines](#code-style-guidelines) 
  in order to set a common programming style for all developers working on the code.

Note that contribution workflows themselves (e.g. pull requests, etc.) are described in another document 
([FIWARE Contribution Requirements](https://fiware-requirements.readthedocs.io/en/latest/)).

## How to contribute

If you'd like to contribute, start by searching through the [issues](https://github.com/scorpiobroker/fiware-orion/issues) and
[pull requests](https://github.com/telefonicaid/fiware-orion/pulls) to see whether someone else has raised a similar idea or
question.

If you don't see your idea listed, and you think it fits into the goals of this guide, do one of the following:

-   **If your contribution is minor,** such as a typo fix, open a pull request.
-   **If your contribution is major,** such as a new guide, start by opening an issue first. That way, other people can
    weigh in on the discussion before you do any work.

### Pull Request protocol

As explained in ([FIWARE Contribution Requirements](https://fiware-requirements.readthedocs.io/en/latest/)) 
contributions are done using a pull request (PR). The detailed "protocol" used in such PR is described below:

* Direct commits to master branch (even single-line modifications) are not allowed. Every modification has to come as a PR
* In case the PR is implementing/fixing a numbered issue, the issue number has to be referenced in the body of the PR at creation time
* Anybody is welcome to provide comments to the PR (either direct comments or using the review feature offered by Github)
* Use *code line comments* instead of *general comments*, for traceability reasons (see comments lifecycle below)
* Comments lifecycle
  * Comment is created, initiating a *comment thread*
  * New comments can be added as responses to the original one, starting a discussion
  * After discussion, the comment thread ends in one of the following ways:
    * `Fixed in <commit hash>` in case the discussion involves a fix in the PR branch (which commit hash is 
       included as reference)
    * `NTC`, if finally nothing needs to be done (NTC = Nothing To Change)
 * PR can be merged when the following conditions are met:
    * All comment threads are closed
    * All the participants in the discussion have provided a `LGTM` general comment (LGTM = Looks good to me)
 * Self-merging is not allowed (except in rare and justified circumstances)

Some additional remarks to take into account when contributing with new PRs:

* PR must include not only code contributions, but their corresponding pieces of documentation (new or modifications to existing one) and tests
* PR modifications must pass full regression based on existing test (unit, functional, memory, e2e) in addition to whichever new test added due to the new functionality
* PR should be of an appropriated size that makes review achievable. Too large PRs could be closed with a "please, redo the work in smaller pieces" without any further discussing

## Community

Discussions about the Open Source Guides take place on this repository's
[Issues](https://github.com/scorpiobroker/scorpiobroker/issues) and [Pull Requests](https://github.com/scorpiobroker/scorpiobroker/pulls)
sections. Anybody is welcome to join these conversations.

Wherever possible, do not take these conversations to private channels, including contacting the maintainers directly.
Keeping communication public means everybody can benefit and learn from the conversation.

## Dependency guidelines

* ALL dependency must be done through Maven only. All dependencies MUST be available on a public Maven repositories.
* We are only accepting new depencies which are released under a well recognized [OSS license](https://opensource.org/licenses)
* Scorpio only relies on the Central Maven Repository at the moment. New repositories MUST be added in the corresponding pom.xml. .m2 configs are not acceptable.
* We will NOT accept any contribution that includes binary files
* We are not using any platform dependent depdencies at the moment. If any platform based depdencies are introduced the following points must be met.
  - Maven profiles MUST be used to control the dependencies.
  - Profiles MUST include an activation based on the platform. So that a default build on the host OS just works as default. [Maven profiles](https://maven.apache.org/guides/introduction/introduction-to-profiles.html)
  - Support for Windows and Linux 64Bit is a MUST. 32Bit support is a SHOULD as 32Bit is coming to an end. 
  - Linux ARM64 is considered a SHOULD at the moment but might become a MUST. Linux ARM hf is a weak SHOULD.
  - Support for Mac is considered a SHOULD as we recognize that testing on Mac OS/OS X is legally not possible without buying a Mac. 
  - All dependencies MUST work relatively out of the box with the standard build. Relatively means e.g. 
     - We will not accept something which requires an installation/compilation manual. 
     - An apt-get on Linux is fine as this is the common way in Linux. 
	BEST is that the native library is packaged with the Java dependency. 
  - If there is a platform specific optimization or something similar (e.g. hardware optimized libraries), it of course only has to be provided for that platform.


## Code style guidelines

### Filesystem layout 
* Scorpio uses Maven as build tool. All contributions MUST follow the file structure dictated by Maven.
* Additional resources like config files MUST be provided in the resources folder.


### Code style guidelines

Note that currently not of all existing Scorpio code base conforms to these rules. There are some parts of the code that were 
written before the guidelines were established. However, all new code contributions MUST follow these rules and, eventually, old code will be modified to conform to the guidelines.

#### ‘MUST follow’ rules 

* There shall be NO compiler warnings. Code needs to be clean. If you have a warning that you consciously want to ignore use the @ignorewarning annotation and comment it. 
* Public methods shall always have comments and javadoc 
* A Logger shall always be used. NO system.out.println()
* Exceptions shall always be handled. Empty catch blocks are not acceptable. If nothing can be done log the error.
* Avoid commits that only change the formatting of a file.
##### Copyright header
All .java files MUST contain the following copyright header
```
/* 
*          Scorpio Broker
* 
*   file: FILE NAME 
* 
* Authors: The Scorpio Development Team (scorpiobroker@neclab.eu) 
*          

* 
*          NEC Laboratories Europe GmbH --- PROPRIETARY INFORMATION 
* 
* The software and its source code contain valuable trade secrets and shall be maintained in
* confidence and treated as confidential information. The software may only be used for evaluation 
* and/or testing purposes, unless otherwise explicitly stated in the terms of a license agreement or 
* nondisclosure agreement with NEC Laboratories Europe GmbH.

* Any unauthorized publication, transfer to third parties or duplication of the object or source code –
* either totally or in part - is strictly prohibited.
*      Copyright (c) 2021 NEC Laboratories Europe GmbH  All Rights Reserved. 
* 
* NEC Laboratories Europe GmbH DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
* IMPLIED, INCLUDING BUT NOT LIMITED TO IMPLIED WARRANTIES OF MERCHANTABILITY 
* AND FITNESS FOR A PARTICULAR PURPOSE AND THE WARRANTY AGAINST LATENT 
* DEFECTS, WITH RESPECT TO THE PROGRAM AND THE ACCOMPANYING 
* DOCUMENTATION. 
* 
* NO LIABILITIES FOR CONSEQUENTIAL DAMAGES:
* IN NO EVENT SHALL NEC Laboratories Europe GmbH or ANY OF ITS SUBSIDIARIES BE 
* LIABLE FOR ANY DAMAGES WHATSOEVER (INCLUDING, WITHOUT LIMITATION, DAMAGES
* FOR LOSS OF BUSINESS PROFITS, BUSINESS INTERRUPTION, LOSS OF INFORMATION, OR 
* OTHER PECUNIARY LOSS AND INDIRECT, CONSEQUENTIAL, INCIDENTAL, 
* ECONOMIC OR PUNITIVE DAMAGES) ARISING OUT OF THE USE OF OR INABILITY 
* TO USE THIS PROGRAM, EVEN IF NEC Laboratories Europe GmbH HAS BEEN ADVISED OF 
* THE POSSIBILITY OF SUCH DAMAGES. 
* 
*     THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
*/
```
#### 'SHOULD follow’ rules

* Stick to the [Java best practices](https://docs.oracle.com/cd/A97688_16/generic.903/bp/java.htm). 
##### Formatting

Most Scorpio devs use Eclipse with the default auto format settings for Java. It would be highly appreciated to stick to those. We extracted the settings below
We will not reject any PR because of different formatting. However we will reject bad formatting. As an example we have no stakes in the "new line before { or not" discussion.
But we will reject endless lines.

###### Eclipse default auto format settings

<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<profiles version="18">
    <profile kind="CodeFormatterProfile" name="Eclipse 123" version="18">
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_ellipsis" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_enum_declarations" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_allocation_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_at_in_annotation_type_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_for_statment" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.new_lines_at_block_boundaries" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_logical_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_constructor_declaration_parameters" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.insert_new_line_for_parameter" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_package" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_method_invocation" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_parens_in_enum_constant" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_after_imports" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_while" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.insert_new_line_before_root_tags" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_parens_in_annotation_type_member_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_method_declaration_throws" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_switch_statement" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_javadoc_comments" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.indentation.size" value="4"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_postfix_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_enum_constant_declaration" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_for_increments" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_type_arguments" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_arrow_in_switch_default" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_for_inits" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_semicolon_in_for" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.align_with_spaces" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.disabling_tag" value="@formatter:off"/>
        <setting id="org.eclipse.jdt.core.formatter.continuation_indentation" value="2"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_before_code_block" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_switch_case_expressions" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_enum_constants" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_imports" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_at_end_of_method_body" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_after_package" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_multiple_local_declarations" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_if_while_statement" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_enum_constant" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_angle_bracket_in_parameterized_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.indent_root_tags" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_or_operator_multicatch" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.enabling_tag" value="@formatter:on"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_closing_brace_in_block" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.count_line_length_from_starting_position" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_parenthesized_expression_in_return" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_throws_clause_in_method_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_parameter" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_arrow_in_switch_case" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_multiplicative_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_then_statement_on_same_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_field" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_explicitconstructorcall_arguments" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_prefix_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_between_type_declarations" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_brace_in_array_initializer" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_for" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_catch" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_angle_bracket_in_type_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_method" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_switch" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_parameterized_type_references" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_anonymous_type_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_logical_operator" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_parenthesized_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_annotation_declaration_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_enum_constant" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_multiplicative_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.never_indent_line_comments_on_first_column" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_and_in_type_parameter" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_for_inits" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_statements_compare_to_block" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_anonymous_type_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_question_in_wildcard" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_method_invocation_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_switch" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.align_tags_descriptions_grouped" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.line_length" value="80"/>
        <setting id="org.eclipse.jdt.core.formatter.use_on_off_tags" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_method_body_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_brackets_in_array_allocation_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_loop_body_block_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_enum_constant" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_parens_in_method_invocation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_assignment_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_type_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_for" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.preserve_white_space_between_code_and_line_comments" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_local_variable" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_method_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_abstract_method" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_enum_constant_declaration_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.align_variable_declarations_on_columns" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_method_invocation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_union_type_in_multicatch" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_colon_in_for" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_type_declaration_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_at_beginning_of_method_body" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_closing_angle_bracket_in_type_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_else_statement_on_same_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_catch_clause" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_additive_operator" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_parameterized_type_reference" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_array_initializer" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_multiple_field_declarations" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_explicit_constructor_call" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_relational_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_multiplicative_operator" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_anonymous_type_declaration_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_switch_case_expressions" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_shift_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_body_declarations_compare_to_annotation_declaration_header" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_superinterfaces" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_default" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_question_in_conditional" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_block" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_constructor_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_lambda_body" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_at_end_of_code_block" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.compact_else_if" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_type_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_catch" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_method_invocation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_bitwise_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.put_empty_statement_on_new_line" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_parameters_in_constructor_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_type_parameters" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_method_invocation_arguments" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_method_invocation" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_throws_clause_in_constructor_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_compact_loops" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.clear_blank_lines_in_block_comment" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_before_catch_in_try_statement" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_try" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_simple_for_body_on_same_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_at_end_of_file_if_missing" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.clear_blank_lines_in_javadoc_comment" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_relational_operator" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_array_initializer" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_unary_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_expressions_in_array_initializer" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.format_line_comment_starting_on_first_column" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_empty_lines_to_preserve" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_annotation" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_colon_in_case" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_ellipsis" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_additive_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_semicolon_in_try_resources" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_colon_in_assert" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_if" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_type_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_and_in_type_parameter" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_string_concatenation" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_parenthesized_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_line_comments" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_colon_in_labeled_statement" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.text_block_indentation" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.align_type_members_on_columns" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_assignment" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_module_statements" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_body_declarations_compare_to_type_header" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_parens_in_method_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_after_code_block" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.align_tags_names_descriptions" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_enum_constant" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_superinterfaces_in_type_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_if_then_body_block_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_first_class_body_declaration" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_conditional_expression" value="80"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_before_closing_brace_in_array_initializer" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_constructor_declaration_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.format_guardian_clause_on_one_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_if" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.align_assignment_statements_on_columns" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_annotation_on_type" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_block" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_enum_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_block_in_case" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_arrow_in_switch_default" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_constructor_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.insert_new_line_between_different_tags" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_conditional_expression_chain" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_header" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_allocation_expression" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_additive_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_method_invocation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_while" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_switch" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_method_declaration" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.join_wrapped_lines" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_parens_in_constructor_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_conditional_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_switchstatements_compare_to_cases" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_bracket_in_array_allocation_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_synchronized" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_shift_operator" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.align_fields_grouping_blank_lines" value="2147483647"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.new_lines_at_javadoc_boundaries" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_bitwise_operator" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_annotation_type_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_for" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_resources_in_try" value="80"/>
        <setting id="org.eclipse.jdt.core.formatter.use_tabs_only_for_leading_indentations" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_try_clause" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_selector_in_method_invocation" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.never_indent_block_comments_on_first_column" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_code_block_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_synchronized" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_constructor_declaration_throws" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.tabulation.size" value="4"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_bitwise_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_allocation_expression" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_bracket_in_array_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_colon_in_conditional" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_source_code" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_array_initializer" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_try" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_semicolon_in_try_resources" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_field" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_at_in_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.continuation_indentation_for_array_initializer" value="2"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_question_in_wildcard" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_method" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_superclass_in_type_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_superinterfaces_in_enum_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_parenthesized_expression_in_throw" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_assignment_operator" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_labeled_statement" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_not_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_switch" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_superinterfaces" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_method_declaration_parameters" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_type_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_brace_in_array_initializer" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_parenthesized_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_html" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_at_in_annotation_type_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_closing_angle_bracket_in_type_parameters" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_method_delcaration" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_compact_if" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_lambda_body_block_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_empty_lines" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_type_arguments" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_parameterized_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_unary_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_enum_constant" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_annotation" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_enum_declarations" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_empty_array_initializer_on_one_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_switchstatements_compare_to_switch" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_before_else_in_if_statement" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_assignment_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_constructor_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_new_chunk" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_label" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_body_declarations_compare_to_enum_declaration_header" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_bracket_in_array_allocation_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_constructor_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_conditional" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_angle_bracket_in_parameterized_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_method_declaration_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_angle_bracket_in_type_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_cast" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_arrow_in_switch_case" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_assert" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_member_type" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_before_while_in_do_statement" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_logical_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_bracket_in_array_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_angle_bracket_in_parameterized_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_arguments_in_qualified_allocation_expression" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_after_opening_brace_in_array_initializer" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_breaks_compare_to_cases" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_method_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_bitwise_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_if" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_semicolon" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_relational_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_postfix_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_try" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_angle_bracket_in_type_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_cast" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.format_block_comments" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_lambda_arrow" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_method_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.indent_tag_description" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_imple_if_on_one_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_enum_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_parameters_in_method_declaration" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_brackets_in_array_type_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_angle_bracket_in_type_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_string_concatenation" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_semicolon_in_for" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_method_declaration_throws" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_bracket_in_array_allocation_expression" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_after_last_class_body_declaration" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_statements_compare_to_body" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_multiple_fields" value="16"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_enum_constant_arguments" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_simple_while_body_on_same_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_prefix_operator" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_array_initializer" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_logical_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_shift_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_method_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_type_parameters" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_catch" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_between_statement_group_in_switch" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_bracket_in_array_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_comma_in_annotation" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_enum_constant_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.parentheses_positions_in_lambda_declaration" value="common_lines"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_shift_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_between_empty_braces_in_array_initializer" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_colon_in_case" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_multiple_local_declarations" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_simple_do_while_body_on_same_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_annotation_type_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_bracket_in_array_reference" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_enum_declaration_on_one_line" value="one_line_never"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_method_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_outer_expressions_when_nested" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_closing_paren_in_cast" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_enum_constant" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.brace_position_for_type_declaration" value="end_of_line"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_multiplicative_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_before_package" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_for" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_synchronized" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_for_increments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_annotation_type_member_declaration" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.alignment_for_expressions_in_for_loop_header" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_additive_operator" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.keep_simple_getter_setter_on_one_line" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_while" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_enum_constant" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_explicitconstructorcall_arguments" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_paren_in_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_angle_bracket_in_type_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.indent_body_declarations_compare_to_enum_constant_header" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_string_concatenation" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_lambda_arrow" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_brace_in_constructor_declaration" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_constructor_declaration_throws" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.join_lines_in_comments" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_closing_angle_bracket_in_type_parameters" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_question_in_conditional" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.comment.indent_parameter_description" value="false"/>
        <setting id="org.eclipse.jdt.core.formatter.number_of_blank_lines_at_beginning_of_code_block" value="0"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_new_line_before_finally_in_try_statement" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.tabulation.char" value="tab"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_relational_operator" value="insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_comma_in_multiple_field_declarations" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.wrap_before_string_concatenation" value="true"/>
        <setting id="org.eclipse.jdt.core.formatter.blank_lines_between_import_groups" value="1"/>
        <setting id="org.eclipse.jdt.core.formatter.lineSplit" value="120"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_after_opening_paren_in_annotation" value="do not insert"/>
        <setting id="org.eclipse.jdt.core.formatter.insert_space_before_opening_paren_in_switch" value="insert"/>
    </profile>
</profiles>




